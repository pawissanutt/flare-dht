use flare_zrpc::{
    BincodeMsgSerde, ZrpcClient, ZrpcError, ZrpcServerError, ZrpcService,
    ZrpcServiceHander, ZrpcTypeConfig,
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

struct TypeConf;
impl ZrpcTypeConfig for TypeConf {
    type In = BincodeMsgSerde<InputMsg>;

    type Out = BincodeMsgSerde<OutputMsg>;

    type Err = BincodeMsgSerde<ZrpcServerError<MyError>>;

    type ErrInner = MyError;
}

type TestService = ZrpcService<TestHandler, TypeConf>;

type TestClient = ZrpcClient<TypeConf>;

use tracing::info;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test() {
    info!("start");
    let z_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let handler = TestHandler;
    let service =
        TestService::new("test/**".into(), z_session.clone(), handler);
    let _service = service.start().await.unwrap();

    let z_session_2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let client = TestClient::new("test".into(), z_session_2.clone()).await;
    for i in 1..10 {
        info!("call rpc = {}", i);
        let out = client
            .call(InputMsg(i))
            .await
            .expect("return should not error");
        info!("return output = {}", out.0);
        assert!(out.0 == (i * 2) as u64);
    }

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

    info!("closed z_session");
}
