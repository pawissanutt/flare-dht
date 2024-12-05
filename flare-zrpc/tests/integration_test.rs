use std::time::Duration;

use flare_zrpc::{
    AnyMsgSerde, ZrpcClient, ZrpcError, ZrpcServerError, ZrpcService,
    ZrpcServiceHander,
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct InputMsg(i64);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct OutputMsg(u64);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct MyError(String);

struct TestHandler;

#[async_trait::async_trait]
impl ZrpcServiceHander for TestHandler {
    type In = InputMsg;

    type Out = OutputMsg;

    type Err = MyError;

    async fn handle(&self, req: InputMsg) -> Result<OutputMsg, MyError> {
        info!("receive {:?}", req);
        let num = req.0;
        if num > 0 {
            Ok(OutputMsg((num * 2) as u64))
        } else {
            Err(MyError("num should be more than 0".into()))
        }
    }
}

type TestService = ZrpcService<
    TestHandler,
    AnyMsgSerde<InputMsg>,
    AnyMsgSerde<OutputMsg>,
    MyError,
    AnyMsgSerde<ZrpcServerError<MyError>>,
>;
type TestClient = ZrpcClient<
    AnyMsgSerde<InputMsg>,
    AnyMsgSerde<OutputMsg>,
    MyError,
    AnyMsgSerde<ZrpcServerError<MyError>>,
>;

use tracing::info;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test() {
    info!("start");
    let z_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let handler = TestHandler;
    let service = TestService::new("test/1".into(), z_session.clone(), handler);
    let _service = service.start().await.unwrap();

    let z_session_2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let client = TestClient::new("test/1".into(), z_session_2.clone());
    info!("call rpc = 2");
    let out = client
        .call(InputMsg(2))
        .await
        .expect("return should not error");
    info!("return output = {}", out.0);
    assert!(out.0 == 4);

    info!("call rpc = -2");
    let out = client
        .call(InputMsg(-2))
        .await
        .expect_err("return should be error");
    info!("return err = {:?}", out);
    if let ZrpcError::ServerError(_) = out {
    } else {
        panic!("expect error")
    }
    // tokio::time::sleep(Duration::from_millis(5000)).await;
    // info!("closing z_session");
    // z_session.close().await.unwrap();
    // z_session_2.close().await.unwrap();

    info!("closed z_session");
}

// #[traced_test]
// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// async fn test_connection() {
//     info!("start");
//     let config = zenoh::Config::default();
//     // config.set_mode(Some(WhatAmI::Router)).unwrap();
//     let z_session = zenoh::open(config).await.unwrap();
//     // let config = zenoh::Config::default();
//     // let z_session_2 = zenoh::open(config).await.unwrap();
//     let (tx, rx) = flume::bounded(32);
//     z_session
//         .declare_queryable("test/test")
//         .callback(move |query| tx.send(query).unwrap())
//         .await
//         .unwrap();
//     tokio::spawn(async move {
//         info!("spawn");
//         while let Ok(query) = rx.recv_async().await {
//             match query.payload() {
//                 None => info!(
//                     ">> [Queryable ] Received Query '{}'",
//                     query.selector()
//                 ),
//                 Some(query_payload) => {
//                     // Refer to z_bytes.rs to see how to deserialize different types of message
//                     let deserialized_payload = query_payload
//                         .try_to_string()
//                         .unwrap_or_else(|e| e.to_string().into());
//                     info!(
//                         ">> [Queryable ] Received Query '{}' with payload '{}'",
//                         query.selector(),
//                         deserialized_payload
//                     );
//                     query.reply(query.key_expr(), "test").await.unwrap();
//                 }
//             }
//         }
//         info!("end spawn");
//     });

//     let reply = z_session
//         .get("test/test")
//         .payload("payload")
//         .with(flume::bounded(32))
//         .await
//         .unwrap();
//     tokio::time::sleep(Duration::from_millis(2000)).await;
//     while let Ok(reply) = reply.recv_async().await {
//         let out = reply.result().unwrap().payload();
//         info!("reply: {:?}", out);
//     }

//     z_session.close().await.unwrap();
//     // z_session_2.close().await.unwrap();

//     info!("closed z_session");
// }
