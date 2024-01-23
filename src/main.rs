use actix_web::{
    error::ErrorInternalServerError, middleware, web, App, HttpResponse, HttpServer, Responder,
};
use log;
use redis::{FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Debug)]
pub struct Message {
    pub uuid: Uuid,
    pub sms: String,
}

impl ToRedisArgs for Message {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg_fmt(serde_json::to_string(self).expect("Failed to serialize Message"))
    }
}

impl FromRedisValue for Message {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        println!("V {:?}", v);
        let map = HashMap::<String, String>::from_redis_value(v);
        println!("RES {:?}", map);
        match map.is_ok() {
            true => {
                let map = map.unwrap();
                println!("MAP {:?}", map);
                let uuid = map.get("uuid");
                let sms = map.get("sms");
                if let (Some(uuid), Some(sms)) = (uuid, sms) {
                    Ok(Message {
                        uuid: Uuid::parse_str(uuid).unwrap(),
                        sms: sms.to_owned(),
                    })
                } else {
                    Err((redis::ErrorKind::TypeError, "Parse to JSON Failed").into())
                }
            }
            false => {
                let v = redis::from_redis_value::<Vec<u8>>(v);
                match v.is_ok() {
                    true => Ok(serde_json::from_slice(v?.as_slice()).expect("!json")),
                    false => Err((redis::ErrorKind::TypeError, "Parse to JSON Failed").into()),
                }
            }
        }
    }
}

impl Message {
    async fn find_by_uuid(redis: &redis::Client, uuid: &str) -> Option<Self> {
        println!("S {}", uuid);
        let uuid = Uuid::parse_str(uuid).unwrap();
        println!("U {}", uuid);

        let mut conn = redis
            .get_connection_manager()
            .await
            .map_err(ErrorInternalServerError)
            .unwrap();

        redis::Cmd::hgetall(uuid.to_string())
            .query_async::<_, Message>(&mut conn)
            .await
            .map_err(ErrorInternalServerError)
            .ok()
    }
}

mod foo;

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    log::info!("starting HTTP server at http://localhost:8080");

    let redis = redis::Client::open("redis://127.0.0.1:6379").unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(redis.clone()))
            .wrap(middleware::Logger::default())
    })
    .workers(2)
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use crate::foo::TestContext;
    use crate::Message;
    use actix_web::error::ErrorInternalServerError;
    use uuid::Uuid;

    #[actix_web::test]
    async fn find_by_uuid() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "info");
        std::env::set_var("RUST_BACKTRACE", "1");
        dotenv::dotenv().ok();

        let ctx = TestContext::new();

        let u = Uuid::new_v4();

        let message = Message {
            uuid: Uuid::new_v4(),
            sms: "redis".into(),
        };

        let mut conn = ctx
            .client
            .get_connection_manager()
            .await
            .map_err(ErrorInternalServerError)
            .unwrap();

        redis::Cmd::hset_multiple(
            format!("message:{}", message.uuid),
            &[
                ("uuid", message.uuid.to_string()),
                ("sms", "foo".to_owned()),
            ],
        )
        .query_async::<_, ()>(&mut conn)
        .await
        .map_err(ErrorInternalServerError)
        .unwrap();

        let res = Message::find_by_uuid(&ctx.client, &message.uuid.to_string()).await;

        // Test
        assert!(res.is_some());
        assert_eq!(res.unwrap().uuid, u);

        Ok(())
    }
}
