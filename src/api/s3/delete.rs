use hyper::{Request, Response, StatusCode};

use garage_util::data::*;
use garage_util::time::*;

use garage_model::garage::Garage;
use garage_model::s3::object_table::*;

use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::put::next_timestamp;
use crate::xml as s3_xml;

/// Check if an object is under retention.
/// If `extend_to` is Some and the object has a current retention-until that is
/// earlier than `extend_to`, the extension is allowed (returns Ok).
pub(crate) async fn retention_check(
	garage: &Garage,
	bucket_id: Uuid,
	key: &str,
	extend_to: Option<&str>,
) -> Result<(), Error> {
	let object = match garage.object_table.get(&bucket_id, &key.to_string()).await? {
		Some(o) => o,
		None => return Ok(()),
	};

	let latest_complete = object.versions().iter().rev().find(|v| {
		matches!(&v.state, ObjectVersionState::Complete(ObjectVersionData::Inline(..)))
	}).or_else(|| {
		object.versions().iter().rev().find(|v| {
			matches!(&v.state, ObjectVersionState::Complete(ObjectVersionData::FirstBlock(..)))
		})
	});

	let meta = match latest_complete {
		Some(v) => match &v.state {
			ObjectVersionState::Complete(ObjectVersionData::Inline(m, _))
			| ObjectVersionState::Complete(ObjectVersionData::FirstBlock(m, _)) => m,
			_ => return Ok(()),
		},
		None => return Ok(()),
	};

	let inner = match &meta.encryption {
		ObjectVersionEncryption::Plaintext { inner } => inner,
		ObjectVersionEncryption::SseC { .. } => {
			return Ok(());
		}
	};

	let until_str = match inner.headers.iter()
		.find(|(name, _)| name.as_str() == "x-amz-meta-retention-until")
	{
		Some((_, val)) => val,
		None => return Ok(()),
	};

	let until_msec = rfc3339_to_msec(until_str)?;
	if now_msec() < until_msec {
		// Check if caller is requesting a retention extension
		if let Some(ext_to) = extend_to {
			let ext_msec = rfc3339_to_msec(ext_to)?;
			if ext_msec > until_msec {
				return Ok(());
			}
		}
		garage.lock_manager.metrics.retention_blocked_counter.add(1);
		return Err(Error::ObjectUnderRetention(until_str.clone()));
	}
	Ok(())
}

async fn handle_delete_internal(ctx: &ReqCtx, key: &str) -> Result<(Uuid, Uuid), Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;

	if garage.config.s3_api.lock_enabled() {
		retention_check(garage, *bucket_id, key, None).await?;

		let _lock_guard = garage
			.lock_manager
			.acquire_distributed(*bucket_id, key)
			.await
			.map_err(|_| Error::SlowDown)?;
	}

	let object = garage
		.object_table
		.get(bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?; // No need to delete

	let del_timestamp = next_timestamp(Some(&object));
	let del_uuid = gen_uuid();

	let deleted_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| !matches!(&v.state, ObjectVersionState::Aborted))
		.or_else(|| object.versions().iter().next_back());
	let deleted_version = match deleted_version {
		Some(dv) => dv.uuid,
		None => {
			warn!("Object has no versions: {:?}", object);
			Uuid::from([0u8; 32])
		}
	};

	let object = Object::new(
		*bucket_id,
		key.into(),
		vec![ObjectVersion {
			uuid: del_uuid,
			timestamp: del_timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::DeleteMarker),
		}],
	);

	garage.object_table.insert(&object).await?;

	Ok((deleted_version, del_uuid))
}

pub async fn handle_delete(ctx: ReqCtx, key: &str) -> Result<Response<ResBody>, Error> {
	match handle_delete_internal(&ctx, key).await {
		Ok(_) | Err(Error::NoSuchKey) => Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(empty_body())
			.unwrap()),
		Err(e) => Err(e),
	}
}

pub async fn handle_delete_objects(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let body = req.into_body().collect().await?;

	let cmd_xml = roxmltree::Document::parse(std::str::from_utf8(&body)?)?;
	let cmd = parse_delete_objects_xml(&cmd_xml).ok_or_bad_request("Invalid delete XML query")?;

	let mut ret_deleted = Vec::new();
	let mut ret_errors = Vec::new();

	for obj in cmd.objects.iter() {
		match handle_delete_internal(&ctx, &obj.key).await {
			Ok((deleted_version, delete_marker_version)) => {
				if cmd.quiet {
					continue;
				}
				ret_deleted.push(s3_xml::Deleted {
					key: s3_xml::Value(obj.key.clone()),
					version_id: s3_xml::Value(hex::encode(deleted_version)),
					delete_marker_version_id: s3_xml::Value(hex::encode(delete_marker_version)),
				});
			}
			Err(e) => {
				ret_errors.push(s3_xml::DeleteError {
					code: s3_xml::Value(e.aws_code().to_string()),
					key: Some(s3_xml::Value(obj.key.clone())),
					message: s3_xml::Value(format!("{}", e)),
					version_id: None,
				});
			}
		}
	}

	let xml = s3_xml::to_xml_with_header(&s3_xml::DeleteResult {
		xmlns: (),
		deleted: ret_deleted,
		errors: ret_errors,
	})?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(string_body(xml))?)
}

struct DeleteRequest {
	quiet: bool,
	objects: Vec<DeleteObject>,
}

struct DeleteObject {
	key: String,
}

fn parse_delete_objects_xml(xml: &roxmltree::Document) -> Option<DeleteRequest> {
	let mut ret = DeleteRequest {
		quiet: false,
		objects: vec![],
	};

	let root = xml.root();
	let delete = root.children().find(|n| n.is_element())?;

	if !delete.has_tag_name("Delete") {
		return None;
	}

	for item in delete.children() {
		// Skip text nodes introduced by formatted XML.
		if !item.is_element() {
			// text nodes are allowed only if they contain whitespace characters only
			if !item.text()?.trim().is_empty() {
				return None;
			}
			continue;
		}

		if item.has_tag_name("Object") {
			let key = item.children().find(|e| e.has_tag_name("Key"))?;
			let key_str = key.text()?;
			ret.objects.push(DeleteObject {
				key: key_str.to_string(),
			});
		} else if item.has_tag_name("Quiet") {
			ret.quiet = item.text()? == "true";
		} else {
			return None;
		}
	}

	Some(ret)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_delete_objects_xml_with_formatting() {
		let body = r#"
			<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			    <Object>
			        <Key>1_746573745f66696c65</Key>
			    </Object>
			    <Quiet>true</Quiet>
			</Delete>
		"#;
		let xml = roxmltree::Document::parse(body).expect("valid delete XML");
		let req = parse_delete_objects_xml(&xml).expect("request should be parsed");

		assert_eq!(req.objects.len(), 1);
		assert_eq!(req.objects[0].key, "1_746573745f66696c65");
		assert!(req.quiet);
	}

	#[test]
	fn parse_delete_objects_xml_rejects_non_whitespace_text_node() {
		let body = r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">oops<Object><Key>1_746573745f66696c65</Key></Object></Delete>"#;
		let xml = roxmltree::Document::parse(body).expect("valid XML");
		let req = parse_delete_objects_xml(&xml);
		assert!(req.is_none());
	}

	#[test]
	fn parse_delete_objects_xml_rejects_pretty_print_with_stray_text() {
		let body = r#"
			<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			    oops
			    <Object>
			        <Key>1_746573745f66696c65</Key>
			    </Object>
			</Delete>
		"#;
		let xml = roxmltree::Document::parse(body).expect("valid XML");
		let req = parse_delete_objects_xml(&xml);
		assert!(req.is_none());
	}

	#[test]
	fn parse_delete_objects_xml_accepts_compact_valid_xml() {
		let body = r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>1_746573745f66696c65</Key></Object><Quiet>false</Quiet></Delete>"#;
		let xml = roxmltree::Document::parse(body).expect("valid XML");
		let req = parse_delete_objects_xml(&xml).expect("request should be parsed");
		assert_eq!(req.objects.len(), 1);
		assert_eq!(req.objects[0].key, "1_746573745f66696c65");
		assert!(!req.quiet);
	}
}
