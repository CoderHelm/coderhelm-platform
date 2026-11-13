//! Just enough of HCL to find where a value is set: the blocks of a
//! Terraform or OpenTofu file (`resource`, `module`, `variable`, `locals`),
//! their attributes with the line each is on, and the simple expressions a
//! value is usually written as (a literal, `var.x`, `local.x`, a string
//! with `${...}` in it). Anything else is kept as its text and treated as
//! unknown; nothing is evaluated.

/// One attribute: `name = expr`, and the 1-based line it starts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr {
    pub name: String,
    pub expr: String,
    pub line: u32,
}

/// One block: `kind "label" "label" { ... }`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Block {
    pub kind: String,
    pub labels: Vec<String>,
    pub line: u32,
    pub end_line: u32,
    pub attrs: Vec<Attr>,
    pub blocks: Vec<Block>,
}

impl Block {
    pub fn attr(&self, name: &str) -> Option<&Attr> {
        self.attrs.iter().find(|a| a.name == name)
    }
    pub fn label(&self, i: usize) -> &str {
        self.labels.get(i).map(String::as_str).unwrap_or("")
    }
    pub fn child(&self, kind: &str) -> Option<&Block> {
        self.blocks.iter().find(|b| b.kind == kind)
    }
}

struct P<'a> {
    s: &'a [u8],
    i: usize,
    line: u32,
    /// Inside `{ k = v, k2: v2 }`: commas end a value, `:` assigns.
    object: bool,
}

impl P<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn at(&self, t: &str) -> bool {
        self.s[self.i..].starts_with(t.as_bytes())
    }
    fn bump(&mut self) -> Option<u8> {
        let c = self.peek()?;
        if c == b'\n' {
            self.line += 1;
        }
        self.i += 1;
        Some(c)
    }
    fn skip_line(&mut self) {
        while let Some(c) = self.peek() {
            if c == b'\n' {
                break;
            }
            self.i += 1;
        }
    }
    fn skip_block_comment(&mut self) {
        while self.peek().is_some() && !self.at("*/") {
            self.bump();
        }
        self.i = (self.i + 2).min(self.s.len());
    }
    /// Whitespace, newlines and comments. In an object, commas too.
    fn skip_space(&mut self, newlines: bool) {
        loop {
            match self.peek() {
                Some(b' ' | b'\t' | b'\r') => self.i += 1,
                Some(b',') if self.object => self.i += 1,
                Some(b'\n') if newlines => {
                    self.bump();
                }
                Some(b'#') => self.skip_line(),
                Some(b'/') if self.at("//") => self.skip_line(),
                Some(b'/') if self.at("/*") => self.skip_block_comment(),
                _ => break,
            }
        }
    }
    fn ident(&mut self) -> Option<String> {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' || c == b'-' || c == b'.' {
                self.i += 1;
            } else {
                break;
            }
        }
        (self.i > start).then(|| String::from_utf8_lossy(&self.s[start..self.i]).to_string())
    }
    /// At `"`: through the closing quote, `${ ... }` and escapes included.
    fn string(&mut self) {
        self.bump();
        while let Some(c) = self.peek() {
            match c {
                b'\\' => {
                    self.bump();
                    self.bump();
                }
                b'"' => {
                    self.bump();
                    return;
                }
                b'$' | b'%' if self.at("${") || self.at("%{") => {
                    self.i += 2;
                    let mut depth = 1;
                    while let Some(c) = self.peek() {
                        match c {
                            b'"' => {
                                self.string();
                                continue;
                            }
                            b'{' => depth += 1,
                            b'}' => {
                                depth -= 1;
                                if depth == 0 {
                                    self.bump();
                                    break;
                                }
                            }
                            _ => {}
                        }
                        self.bump();
                    }
                }
                _ => {
                    self.bump();
                }
            }
        }
    }
    fn heredoc(&mut self) {
        self.i += 2;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        let Some(tag) = self.ident() else {
            return;
        };
        self.skip_line();
        while self.peek().is_some() {
            self.bump();
            let start = self.i;
            self.skip_line();
            let l = String::from_utf8_lossy(&self.s[start..self.i]);
            if l.trim() == tag {
                return;
            }
        }
    }
    /// A value: to the end of the line at depth 0 (or a comma or the
    /// closing brace of an object).
    fn expr(&mut self) -> String {
        let start = self.i;
        let mut depth = 0i32;
        while let Some(c) = self.peek() {
            match c {
                b'"' => {
                    self.string();
                    continue;
                }
                b'<' if self.at("<<") && depth == 0 => {
                    self.heredoc();
                    continue;
                }
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                b'\n' if depth == 0 => break,
                b',' if depth == 0 && self.object => break,
                b'#' if depth == 0 => break,
                b'/' if depth == 0 && (self.at("//") || self.at("/*")) => break,
                _ => {}
            }
            self.bump();
        }
        String::from_utf8_lossy(&self.s[start..self.i])
            .trim()
            .to_string()
    }
    fn body(&mut self, top: bool) -> (Vec<Attr>, Vec<Block>) {
        let mut attrs = Vec::new();
        let mut blocks = Vec::new();
        loop {
            self.skip_space(true);
            match self.peek() {
                None => break,
                Some(b'}') => {
                    if !top {
                        self.bump();
                        break;
                    }
                    self.bump();
                    continue;
                }
                _ => {}
            }
            let line = self.line;
            let name = if self.peek() == Some(b'"') {
                let start = self.i;
                self.string();
                String::from_utf8_lossy(&self.s[start..self.i])
                    .trim_matches('"')
                    .to_string()
            } else {
                match self.ident() {
                    Some(n) => n,
                    None => {
                        // Not something this reader knows: next line.
                        self.bump();
                        self.skip_line();
                        continue;
                    }
                }
            };
            self.skip_space(false);
            let assigns = match self.peek() {
                Some(b'=') => !self.at("=="),
                Some(b':') => self.object,
                _ => false,
            };
            if assigns {
                self.bump();
                self.skip_space(false);
                let expr = self.expr();
                attrs.push(Attr { name, expr, line });
                continue;
            }
            let mut labels = Vec::new();
            loop {
                self.skip_space(false);
                match self.peek() {
                    Some(b'"') => {
                        let start = self.i;
                        self.string();
                        labels.push(
                            String::from_utf8_lossy(&self.s[start..self.i])
                                .trim_matches('"')
                                .to_string(),
                        );
                    }
                    Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                        if let Some(l) = self.ident() {
                            labels.push(l);
                        }
                    }
                    _ => break,
                }
            }
            if self.peek() == Some(b'{') {
                self.bump();
                let (a, b) = self.body(false);
                blocks.push(Block {
                    kind: name,
                    labels,
                    line,
                    end_line: self.line,
                    attrs: a,
                    blocks: b,
                });
            } else {
                self.skip_line();
            }
        }
        (attrs, blocks)
    }
}

/// The blocks of a `.tf` file. Text that is not HCL gives no blocks.
pub fn parse(src: &str) -> Vec<Block> {
    let mut p = P {
        s: src.as_bytes(),
        i: 0,
        line: 1,
        object: false,
    };
    p.body(true).1
}

/// The assignments of a `.tfvars` file (or any file's top level).
pub fn assignments(src: &str) -> Vec<Attr> {
    let mut p = P {
        s: src.as_bytes(),
        i: 0,
        line: 1,
        object: false,
    };
    p.body(true).0
}

/// The entries of an object value (`{ Name = "orders", Env = "prod" }`),
/// each with its line counted from `first_line`.
pub fn object(expr: &str, first_line: u32) -> Vec<Attr> {
    let t = expr.trim();
    let Some(inner) = t.strip_prefix('{').and_then(|x| x.strip_suffix('}')) else {
        return Vec::new();
    };
    let mut p = P {
        s: inner.as_bytes(),
        i: 0,
        line: first_line,
        object: true,
    };
    p.body(true).0
}

/// One piece of a string with `${...}` in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Part {
    Text(String),
    /// What stands between `${` and `}`.
    Ref(String),
}

/// A value as it is usually written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `"orders"`, `16`, `true`.
    Lit(String),
    /// `var.name`.
    Var(String),
    /// `local.name`.
    Local(String),
    /// `each.key`, `each.value.engine_version`, `count.index`.
    Each(String),
    /// `"${var.env}-orders"`.
    Template(Vec<Part>),
    /// Anything else, as written.
    Other(String),
}

fn word(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn reference(t: &str) -> Value {
    if let Some(v) = t.strip_prefix("var.").filter(|v| word(v)) {
        return Value::Var(v.to_string());
    }
    if let Some(v) = t.strip_prefix("local.").filter(|v| word(v)) {
        return Value::Local(v.to_string());
    }
    if t.starts_with("each.") || t.starts_with("count.") {
        return Value::Each(t.to_string());
    }
    Value::Other(t.to_string())
}

/// Reads an expression's text.
pub fn value(expr: &str) -> Value {
    let t = expr.trim();
    if let Some(inner) = t
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .filter(|x| !x.contains('"') || x.contains("${"))
    {
        if !inner.contains("${") {
            return Value::Lit(inner.to_string());
        }
        let mut parts = Vec::new();
        let mut rest = inner;
        while let Some(at) = rest.find("${") {
            if at > 0 {
                parts.push(Part::Text(rest[..at].to_string()));
            }
            let after = &rest[at + 2..];
            let mut depth = 1;
            let mut end = after.len();
            for (i, c) in after.char_indices() {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            parts.push(Part::Ref(after[..end].trim().to_string()));
            rest = after.get(end + 1..).unwrap_or("");
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_string()));
        }
        // "${var.x}" alone is the reference itself.
        if let [Part::Ref(r)] = parts.as_slice() {
            return reference(r);
        }
        return Value::Template(parts);
    }
    if t == "true"
        || t == "false"
        || (!t.is_empty() && t.chars().all(|c| c.is_ascii_digit() || c == '.'))
    {
        return Value::Lit(t.to_string());
    }
    reference(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TF: &str = r#"
# The orders database.
resource "aws_db_instance" "orders" {
  identifier_prefix = "orders-"
  engine            = "postgres" # pinned
  engine_version    = var.db_engine_version
  tags = {
    Name        = "orders"
    Environment = var.env
  }
  lifecycle {
    ignore_changes = [password]
  }
  user_data = <<-EOT
    echo "resource \"nope\" {"
  EOT
}

/* a module call */
module "aurora" {
  source  = "../../modules/aurora"
  name    = "${var.env}-ledger"
  version_pin = "16.4"
  for_each = toset(["a", "b"])
}

variable "db_engine_version" {
  type    = string
  default = "16.4"
}

locals {
  env = "prod"
}
"#;

    #[test]
    fn blocks_and_attributes_come_with_their_lines() {
        let b = parse(TF);
        let kinds: Vec<&str> = b.iter().map(|x| x.kind.as_str()).collect();
        assert_eq!(kinds, vec!["resource", "module", "variable", "locals"]);
        let db = &b[0];
        assert_eq!(db.labels, vec!["aws_db_instance", "orders"]);
        assert_eq!(db.line, 3);
        assert_eq!(db.attr("engine").unwrap().expr, "\"postgres\"");
        let ev = db.attr("engine_version").unwrap();
        assert_eq!((ev.expr.as_str(), ev.line), ("var.db_engine_version", 6));
        assert!(db.child("lifecycle").is_some());
        // A heredoc that looks like a block is text, not a block.
        assert_eq!(b.len(), 4);
        assert!(db.attr("user_data").is_some());
        let tags = db.attr("tags").unwrap();
        let t = object(&tags.expr, tags.line);
        assert_eq!(t.len(), 2);
        assert_eq!(
            (t[0].name.as_str(), t[0].expr.as_str(), t[0].line),
            ("Name", "\"orders\"", 8)
        );
        assert_eq!(b[1].label(0), "aurora");
        assert_eq!(b[1].attr("for_each").unwrap().expr, "toset([\"a\", \"b\"])");
        assert_eq!(b[2].attr("default").unwrap().expr, "\"16.4\"");
    }

    #[test]
    fn values_are_read_as_written() {
        assert_eq!(value("\"orders\""), Value::Lit("orders".into()));
        assert_eq!(value("16"), Value::Lit("16".into()));
        assert_eq!(value("var.env"), Value::Var("env".into()));
        assert_eq!(value("\"${var.env}\""), Value::Var("env".into()));
        assert_eq!(value("local.name"), Value::Local("name".into()));
        assert_eq!(
            value("each.value.engine_version"),
            Value::Each("each.value.engine_version".into())
        );
        assert_eq!(
            value("\"${var.env}-ledger\""),
            Value::Template(vec![
                Part::Ref("var.env".into()),
                Part::Text("-ledger".into())
            ])
        );
        assert!(matches!(value("coalesce(var.a, \"x\")"), Value::Other(_)));
        assert!(matches!(value("var.map[\"k\"]"), Value::Other(_)));
    }

    #[test]
    fn tfvars_and_one_line_objects_read_too() {
        let v = assignments(
            "env = \"prod\"\ndb_engine_version = \"16.4\" # live\nsizes = { a = 1, b = 2 }\n",
        );
        assert_eq!(v.len(), 3);
        assert_eq!(
            (v[1].name.as_str(), v[1].expr.as_str(), v[1].line),
            ("db_engine_version", "\"16.4\"", 2)
        );
        let o = object(&v[2].expr, v[2].line);
        assert_eq!(
            o.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(parse("not hcl at all {{{").len() <= 1);
        assert!(parse("").is_empty());
    }
}
