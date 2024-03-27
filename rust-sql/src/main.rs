use std::env;
use postgres::{self, Client, NoTls};
use postgres_protocol::escape::*;

fn execute(client: &mut Client, query: &str) -> Result<(), postgres::Error> {
    println!("{}", query);
    client.batch_execute(query)
}

fn create_peer(client: &mut Client, name: &str, host: &str, port: u16, database: &str, user: &str, password: &str) -> Result<(), postgres::Error> {
    execute(client, &format!("CREATE PEER {} FROM POSTGRES WITH (
    host = {},
    port = {},
    database = {},
    user = {},
    password = {}
)",
        escape_identifier(name),
        escape_literal(host),
        port,
        escape_literal(database),
        escape_literal(user),
        escape_literal(password),
    ))
}

fn create_mirror(client: &mut Client, name: &str, peer1: &str, peer2: &str, tables: &[String]) -> Result<(), postgres::Error> {
    let mut table_mapping = Vec::with_capacity(tables.len());
    for table in tables {
        table_mapping.push(format!("{}:{}", escape_identifier(table), escape_identifier(table)));
    }
    execute(client, &format!(
        "create mirror {} from {} to {} with table mapping ({}) with (do_initial_copy = true)",
        escape_identifier(name),
        escape_identifier(peer1),
        escape_identifier(peer2),
        table_mapping.join(","),
    ))
}

fn drop_mirror(client: &mut Client, name: &str) -> Result<(), postgres::Error> {
    execute(client, &format!("drop mirror {}", escape_identifier(name)))
}

fn main() {
    let mut args = env::args();
    args.next();
    let Some(connstr) = args.next() else {
        println!("rust-peerdb-example CONNECTIONSTRING create-peer name host port database user password");
        println!("rust-peerdb-example CONNECTIONSTRING create-mirror name peer1 peer2 schema.table...");
        println!("rust-peerdb-example CONNECTIONSTRING drop-mirror name");
        return
    };
    let mut client = Client::connect(&connstr, NoTls).expect("could not connect to peerdb-server, first argument should be connection string");
    let cmd = args.next().expect("no command supplied");

    match &cmd[..] {
        "create-peer" => {
            let name = args.next().expect("expected name");
            let host = args.next().expect("expected host");
            let port = args.next().expect("expected port").parse::<u16>().expect("expected port to be 16 bit unsigned integer");
            let database = args.next().expect("expected database name");
            let user = args.next().expect("expected username");
            let password = args.next().expect("expected password");
            create_peer(&mut client, &name, &host, port, &database, &user, &password)
        }
        "create-mirror" => {
            let name = args.next().expect("expected name");
            let peer1 = args.next().expect("expected first peer name");
            let peer2 = args.next().expect("expected second peer name");
            let tables = args.collect::<Vec<String>>();
            create_mirror(&mut client, &name, &peer1, &peer2, &tables)
        }
        "drop-mirror" => {
            let name = args.next().expect("expected name");
            drop_mirror(&mut client, &name)
        }
        _ => panic!("Unknown arg: {}", cmd)
    }.expect("command failed");
}
