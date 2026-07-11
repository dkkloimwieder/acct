//! Minimal pgoutput (logical replication protocol v1) message parser — just
//! enough to extract `trx_line` INSERT events from the feed slot's binary
//! changes.
//!
//! The consumer requests `proto_version '1'`, so transactions arrive whole and
//! in commit order (no in-progress streaming), and tuple columns arrive in
//! text format. Each `pg_logical_slot_peek_binary_changes` row's `data` is one
//! protocol message; a fresh decoding context re-sends Relation messages
//! before the first Insert that needs them, so a relation map built per batch
//! is always complete.

/// One decoded protocol message. Only Relation and Insert carry payload the
/// feed uses; everything else is structural (Begin/Commit) or ignorable
/// (Type/Origin/Message/Truncate).
#[derive(Debug)]
pub enum Message {
    Begin,
    Commit,
    Relation(Relation),
    Insert {
        rel_id: u32,
        /// Column values in relation order; None = SQL NULL.
        columns: Vec<Option<String>>,
    },
    Other(u8),
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("pgoutput parse error: {0}")]
pub struct ParseError(String);

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ParseError> {
        let end = self.pos.checked_add(n).ok_or_else(|| ParseError("overflow".into()))?;
        if end > self.buf.len() {
            return Err(ParseError(format!(
                "truncated message: need {n} bytes at offset {}, have {}",
                self.pos,
                self.buf.len() - self.pos
            )));
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ParseError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, ParseError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn cstr(&mut self) -> Result<String, ParseError> {
        let rest = &self.buf[self.pos..];
        let nul = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| ParseError("unterminated cstring".into()))?;
        let s = String::from_utf8(rest[..nul].to_vec())
            .map_err(|e| ParseError(format!("non-utf8 cstring: {e}")))?;
        self.pos += nul + 1;
        Ok(s)
    }
}

/// Parse one protocol message (one `data` bytea from the peek function).
pub fn parse(data: &[u8]) -> Result<Message, ParseError> {
    let mut r = Reader::new(data);
    let tag = r.u8()?;
    match tag {
        b'B' => Ok(Message::Begin),
        b'C' => Ok(Message::Commit),
        b'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let _replica_identity = r.u8()?;
            let ncols = r.u16()?;
            let mut column_names = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                let _flags = r.u8()?;
                column_names.push(r.cstr()?);
                let _type_oid = r.u32()?;
                let _type_mod = r.i32()?;
            }
            Ok(Message::Relation(Relation { id, namespace, name, column_names }))
        }
        b'I' => {
            let rel_id = r.u32()?;
            let tuple_kind = r.u8()?;
            if tuple_kind != b'N' {
                return Err(ParseError(format!(
                    "insert tuple kind {:?}, expected 'N'",
                    tuple_kind as char
                )));
            }
            let ncols = r.u16()?;
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                match r.u8()? {
                    // SQL NULL, or unchanged-TOAST (updates only; never on insert).
                    b'n' | b'u' => columns.push(None),
                    b't' => {
                        let len = r.i32()?;
                        if len < 0 {
                            return Err(ParseError(format!("negative column length {len}")));
                        }
                        let bytes = r.take(len as usize)?;
                        let s = String::from_utf8(bytes.to_vec())
                            .map_err(|e| ParseError(format!("non-utf8 column value: {e}")))?;
                        columns.push(Some(s));
                    }
                    other => {
                        return Err(ParseError(format!(
                            "unsupported column kind {:?} (binary transfer not requested)",
                            other as char
                        )));
                    }
                }
            }
            Ok(Message::Insert { rel_id, columns })
        }
        // Type / Origin / logical Message / Truncate — nothing the feed needs.
        b'Y' | b'O' | b'M' | b'T' => Ok(Message::Other(tag)),
        other => Err(ParseError(format!("unknown message tag {:?}", other as char))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_relation_and_insert() {
        // Relation: id=100, ns "public", name "trx_line", replident 'd', 2 cols.
        let mut rel = vec![b'R'];
        rel.extend(100u32.to_be_bytes());
        rel.extend(b"public\0");
        rel.extend(b"trx_line\0");
        rel.push(b'd');
        rel.extend(2u16.to_be_bytes());
        for (name, oid) in [("id", 20u32), ("pool_id", 20u32)] {
            rel.push(0);
            rel.extend(name.as_bytes());
            rel.push(0);
            rel.extend(oid.to_be_bytes());
            rel.extend((-1i32).to_be_bytes());
        }
        let Message::Relation(r) = parse(&rel).unwrap() else { panic!("not a relation") };
        assert_eq!(r.id, 100);
        assert_eq!(r.name, "trx_line");
        assert_eq!(r.column_names, vec!["id", "pool_id"]);

        // Insert: rel 100, tuple ('7', NULL).
        let mut ins = vec![b'I'];
        ins.extend(100u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(2u16.to_be_bytes());
        ins.push(b't');
        ins.extend(1i32.to_be_bytes());
        ins.extend(b"7");
        ins.push(b'n');
        let Message::Insert { rel_id, columns } = parse(&ins).unwrap() else {
            panic!("not an insert")
        };
        assert_eq!(rel_id, 100);
        assert_eq!(columns, vec![Some("7".to_string()), None]);
    }

    #[test]
    fn rejects_truncated_and_unknown() {
        assert!(parse(&[b'I', 0, 0]).is_err());
        assert!(parse(&[b'Z']).is_err());
        assert!(matches!(parse(&[b'Y', 1, 2]).unwrap(), Message::Other(b'Y')));
    }
}
