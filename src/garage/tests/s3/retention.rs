use crate::common;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_retention_block_put() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-put");

	let data = ByteStream::from_static(b"Hello world!");
	let future = "2999-01-01T00:00:00.000Z";

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.metadata("retention-until", future)
		.body(data)
		.send()
		.await
		.unwrap();

	let data2 = ByteStream::from_static(b"overwrite");
	let err = ctx
		.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.metadata("retention-until", future)
		.body(data2)
		.send()
		.await
		.unwrap_err();
	let service_err = err.into_service_error();
	assert_eq!(service_err.code(), Some("ObjectUnderRetention"));
}

#[tokio::test]
async fn test_retention_block_delete() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-del");

	let data = ByteStream::from_static(b"Hello world!");
	let future = "2999-01-01T00:00:00.000Z";

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.metadata("retention-until", future)
		.body(data)
		.send()
		.await
		.unwrap();

	let err = ctx
		.client
		.delete_object()
		.bucket(&bucket)
		.key("test")
		.send()
		.await
		.unwrap_err();
	let service_err = err.into_service_error();
	assert_eq!(service_err.code(), Some("ObjectUnderRetention"));
}

#[tokio::test]
async fn test_retention_expired_allows_delete() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-exp");

	let data = ByteStream::from_static(b"Hello world!");
	let past = "2000-01-01T00:00:00.000Z";

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.metadata("retention-until", past)
		.body(data)
		.send()
		.await
		.unwrap();

	ctx.client
		.delete_object()
		.bucket(&bucket)
		.key("test")
		.send()
		.await
		.unwrap();
}

#[tokio::test]
async fn test_retention_no_header_allows_delete() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-nohead");

	let data = ByteStream::from_static(b"Hello world!");

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.body(data)
		.send()
		.await
		.unwrap();

	ctx.client
		.delete_object()
		.bucket(&bucket)
		.key("test")
		.send()
		.await
		.unwrap();
}

#[tokio::test]
async fn test_retention_block_copy() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-copy");

	let data = ByteStream::from_static(b"target object with retention");
	let src_data = ByteStream::from_static(b"source");
	let future = "2999-01-01T00:00:00.000Z";

	// PUT target object with retention
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("target")
		.metadata("retention-until", future)
		.body(data)
		.send()
		.await
		.unwrap();

	// PUT source object (no retention)
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("src")
		.body(src_data)
		.send()
		.await
		.unwrap();

	// COPY src -> target should be blocked by retention
	let err = ctx
		.client
		.copy_object()
		.bucket(&bucket)
		.key("target")
		.copy_source(format!("{}/{}", bucket, "src"))
		.send()
		.await
		.unwrap_err();
	let service_err = err.into_service_error();
	assert_eq!(service_err.code(), Some("ObjectUnderRetention"));
}

#[tokio::test]
async fn test_retention_extension_by_copy() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-ret-ext");

	let data = ByteStream::from_static(b"extendable retention");
	let future = "2999-01-01T00:00:00.000Z";

	// PUT object with retention
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("ext")
		.metadata("retention-until", future)
		.body(data)
		.send()
		.await
		.unwrap();

	// COPY-to-self with REPLACE + later retention-until -> OK (extension)
	let later = "2999-06-15T00:00:00.000Z";
	ctx.client
		.copy_object()
		.bucket(&bucket)
		.key("ext")
		.copy_source(format!("{}/{}", bucket, "ext"))
		.metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
		.metadata("retention-until", later)
		.send()
		.await
		.unwrap();

	// Verify original delete is still blocked (retention extended)
	let err = ctx
		.client
		.delete_object()
		.bucket(&bucket)
		.key("ext")
		.send()
		.await
		.unwrap_err();
	let service_err = err.into_service_error();
	assert_eq!(service_err.code(), Some("ObjectUnderRetention"));
}
