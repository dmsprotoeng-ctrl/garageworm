use crate::common;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_lock_concurrent_put() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-lock-put");

	let data = ByteStream::from_static(b"hello");
	let data2 = ByteStream::from_static(b"world");

	let c1 = ctx.client.clone();
	let b1 = bucket.clone();
	let j1 = tokio::spawn(async move {
		c1.put_object()
			.bucket(&b1)
			.key("concurrent")
			.body(data)
			.send()
			.await
	});

	let c2 = ctx.client.clone();
	let b2 = bucket.clone();
	let j2 = tokio::spawn(async move {
		c2.put_object()
			.bucket(&b2)
			.key("concurrent")
			.body(data2)
			.send()
			.await
	});

	let (r1, r2) = tokio::join!(j1, j2);
	let r1 = r1.unwrap();
	let r2 = r2.unwrap();

	match (r1, r2) {
		(Ok(_), Ok(_)) => {}
		(Err(e), Ok(_)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Ok(_), Err(e)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Err(e1), Err(e2)) => {
			panic!(
				"both requests failed: {:?}, {:?}",
				e1.into_service_error().code(),
				e2.into_service_error().code()
			);
		}
	}
}

#[tokio::test]
async fn test_lock_different_keys_no_conflict() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-lock-diff");

	let data1 = ByteStream::from_static(b"hello");
	let data2 = ByteStream::from_static(b"world");

	let c1 = ctx.client.clone();
	let b1 = bucket.clone();
	let j1 = tokio::spawn(async move {
		c1.put_object()
			.bucket(&b1)
			.key("key-a")
			.body(data1)
			.send()
			.await
	});

	let c2 = ctx.client.clone();
	let b2 = bucket.clone();
	let j2 = tokio::spawn(async move {
		c2.put_object()
			.bucket(&b2)
			.key("key-b")
			.body(data2)
			.send()
			.await
	});

	let (r1, r2) = tokio::join!(j1, j2);
	r1.unwrap().unwrap();
	r2.unwrap().unwrap();
}

#[tokio::test]
async fn test_lock_concurrent_delete() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-lock-del");

	let data = ByteStream::from_static(b"delete-me");
	let data2 = ByteStream::from_static(b"delete-me2");

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("concurrent")
		.body(data)
		.send()
		.await
		.unwrap();

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("concurrent")
		.body(data2)
		.send()
		.await
		.unwrap();

	let c1 = ctx.client.clone();
	let b1 = bucket.clone();
	let j1 = tokio::spawn(async move {
		c1.delete_object()
			.bucket(&b1)
			.key("concurrent")
			.send()
			.await
	});

	let c2 = ctx.client.clone();
	let b2 = bucket.clone();
	let j2 = tokio::spawn(async move {
		c2.delete_object()
			.bucket(&b2)
			.key("concurrent")
			.send()
			.await
	});

	let (r1, r2) = tokio::join!(j1, j2);
	let r1 = r1.unwrap();
	let r2 = r2.unwrap();

	match (r1, r2) {
		(Ok(_), Ok(_)) => {}
		(Err(e), Ok(_)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Ok(_), Err(e)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Err(e1), Err(e2)) => {
			panic!(
				"both requests failed: {:?}, {:?}",
				e1.into_service_error().code(),
				e2.into_service_error().code()
			);
		}
	}
}

#[tokio::test]
async fn test_lock_sequential_put_ok() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-lock-seq");

	let data = ByteStream::from_static(b"first");
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("seq")
		.body(data)
		.send()
		.await
		.unwrap();

	let data2 = ByteStream::from_static(b"second");
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("seq")
		.body(data2)
		.send()
		.await
		.unwrap();

	let res = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key("seq")
		.send()
		.await
		.unwrap();
	assert_bytes_eq!(res.body, b"second");
}

#[tokio::test]
async fn test_lock_multipart_concurrent_create() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-lock-mpu");

	let c1 = ctx.client.clone();
	let b1 = bucket.clone();
	let j1 = tokio::spawn(async move {
		c1.create_multipart_upload()
			.bucket(&b1)
			.key("mpu")
			.send()
			.await
	});

	let c2 = ctx.client.clone();
	let b2 = bucket.clone();
	let j2 = tokio::spawn(async move {
		c2.create_multipart_upload()
			.bucket(&b2)
			.key("mpu")
			.send()
			.await
	});

	let (r1, r2) = tokio::join!(j1, j2);
	let r1 = r1.unwrap();
	let r2 = r2.unwrap();

	match (r1, r2) {
		(Ok(_), Ok(_)) => {}
		(Err(e), Ok(_)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Ok(_), Err(e)) => {
			assert_eq!(e.into_service_error().code(), Some("SlowDown"));
		}
		(Err(e1), Err(e2)) => {
			panic!(
				"both requests failed: {:?}, {:?}",
				e1.into_service_error().code(),
				e2.into_service_error().code()
			);
		}
	}
}
