//! Minimal encoder/decoder for the subset of RESP (REdis Serialization
//! Protocol) used by this server.

/// A RESP value, used for encoding responses sent back to the client.
#[derive(Debug, Clone)]
pub enum RespMessage {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    NullArray,
    Array(Vec<RespMessage>),
}

impl RespMessage {
    pub fn encode(&self) -> String {
        match self {
            RespMessage::SimpleString(s) => format!("+{}\r\n", s),
            RespMessage::Error(s) => format!("-{}\r\n", s),
            RespMessage::Integer(i) => format!(":{}\r\n", i),
            RespMessage::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            RespMessage::NullBulkString => "$-1\r\n".to_string(),
            RespMessage::NullArray => "*-1\r\n".to_string(),
            RespMessage::Array(items) => {
                let mut out = format!("*{}\r\n", items.len());
                for item in items {
                    out.push_str(&item.encode());
                }
                out
            }
        }
    }
}

/// The name of a command sent by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandName {
    Ping,
    Echo,
    Set,
    Get,
    Rpush,
    Lpush,
    Lrange,
    Llen,
    Lpop,
    Blpop,
    Type,
    Xadd,
    Xrange,
}

impl CommandName {
    /// Maps a command name to a `CommandName`, or returns the original
    /// (unrecognized) name back as the error.
    fn parse(name: &str) -> Result<CommandName, String> {
        match name.to_uppercase().as_str() {
            "PING" => Ok(CommandName::Ping),
            "ECHO" => Ok(CommandName::Echo),
            "SET" => Ok(CommandName::Set),
            "GET" => Ok(CommandName::Get),
            "RPUSH" => Ok(CommandName::Rpush),
            "LPUSH" => Ok(CommandName::Lpush),
            "LRANGE" => Ok(CommandName::Lrange),
            "LLEN" => Ok(CommandName::Llen),
            "LPOP" => Ok(CommandName::Lpop),
            "BLPOP" => Ok(CommandName::Blpop),
            "TYPE" => Ok(CommandName::Type),
            "XADD" => Ok(CommandName::Xadd),
            "XRANGE" => Ok(CommandName::Xrange),
            _ => Err(name.to_string()),
        }
    }
}

/// A command sent by a client, e.g. `ECHO hey` -> `Command { name: Echo, args: ["hey"] }`.
#[derive(Debug, Clone)]
pub struct Command {
    pub name: CommandName,
    pub args: Vec<String>,
}

impl Command {
    /// Parses a RESP array of bulk strings (the shape every client command
    /// takes) into a `Command`, e.g. `*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n` -> `ECHO hey`.
    ///
    /// On failure, returns the RESP error message to send back to the client.
    pub fn parse(input: &[u8]) -> Result<Command, RespMessage> {
        let mut parts = parse_bulk_string_array(input)
            .ok_or_else(|| RespMessage::Error("ERR Protocol error: invalid request".to_string()))?
            .into_iter();

        // parse_bulk_string_array guarantees at least one element.
        let name = CommandName::parse(&parts.next().unwrap())
            .map_err(|name| RespMessage::Error(format!("ERR unknown command '{}'", name)))?;

        Ok(Command {
            name,
            args: parts.collect(),
        })
    }
}

/// Parses a RESP array of bulk strings into its component strings, e.g.
/// `*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n` -> `["ECHO", "hey"]`. Returns `None` if
/// the input is malformed or the array is empty.
fn parse_bulk_string_array(input: &[u8]) -> Option<Vec<String>> {
    let text = std::str::from_utf8(input).ok()?;
    let mut lines = text.split("\r\n");

    let header = lines.next()?;
    let count: usize = header.strip_prefix('*')?.parse().ok()?;
    if count == 0 {
        return None;
    }

    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        let len_line = lines.next()?;
        let len: usize = len_line.strip_prefix('$')?.parse().ok()?;

        let data = lines.next()?;
        if data.len() != len {
            return None;
        }

        parts.push(data.to_string());
    }

    Some(parts)
}
