//! Recursive-descent SQL parser for virtual system catalog & ORM introspection queries.

use crate::types::{normalize_ident, CatalogValue, VirtualCatalog};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    QuotedIdent(String),
    StringLit(String),
    Number(String),
    Param(usize),
    SysVar(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    Comma,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Amp,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    DoubleColon,
    Concat,
    ContainsArr,
    ContainedByArr,
    RegexMatch,
    RegexNotMatch,
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Line comment --
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment /* ... */
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }

        // PostgreSQL escape string E'...'
        if (b == b'E' || b == b'e') && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
            i += 1; // fall through to string literal parser
            let lit = read_quoted(input, &mut i, b'\'')?;
            tokens.push(Token::StringLit(lit));
            continue;
        }

        match b {
            b'\'' => {
                let lit = read_quoted(input, &mut i, b'\'')?;
                tokens.push(Token::StringLit(lit));
            }
            b'"' => {
                let ident = read_quoted(input, &mut i, b'"')?;
                tokens.push(Token::QuotedIdent(ident));
            }
            b'`' => {
                let ident = read_quoted(input, &mut i, b'`')?;
                tokens.push(Token::QuotedIdent(ident));
            }
            b'$' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i > start {
                    let idx: usize = input[start..i].parse().unwrap_or(1);
                    tokens.push(Token::Param(idx));
                } else {
                    // Dollar-quoted tag $tag$...$tag$
                    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                    {
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i] == b'$' {
                        i += 1;
                        let tag = &input[start - 1..i];
                        let body_start = i;
                        if let Some(rel_end) = input[i..].find(tag) {
                            tokens
                                .push(Token::StringLit(input[body_start..i + rel_end].to_string()));
                            i += rel_end + tag.len();
                        } else {
                            return Err("Unterminated dollar-quoted string".into());
                        }
                    } else {
                        return Err("Unexpected '$' token".into());
                    }
                }
            }
            b'?' => {
                tokens.push(Token::Param(0));
                i += 1;
            }
            b'@' if i + 1 < bytes.len() && bytes[i + 1] == b'@' => {
                i += 2;
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
                {
                    i += 1;
                }
                tokens.push(Token::SysVar(input[start..i].to_ascii_lowercase()));
            }
            b'@' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                tokens.push(Token::ContainsArr);
                i += 2;
            }
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b'[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            b']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            b'.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            b'*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            b'+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            b'/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            b'%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            b'&' => {
                tokens.push(Token::Amp);
                i += 1;
            }
            b'=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::Neq);
                i += 2;
            }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'~' => {
                tokens.push(Token::RegexNotMatch);
                if i + 2 < bytes.len() && bytes[i + 2] == b'*' {
                    i += 3;
                } else {
                    i += 2;
                }
            }
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                tokens.push(Token::Neq);
                i += 2;
            }
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::Lte);
                i += 2;
            }
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'@' => {
                tokens.push(Token::ContainedByArr);
                i += 2;
            }
            b'<' => {
                tokens.push(Token::Lt);
                i += 1;
            }
            b'>' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::Gte);
                i += 2;
            }
            b'>' => {
                tokens.push(Token::Gt);
                i += 1;
            }
            b':' if i + 1 < bytes.len() && bytes[i + 1] == b':' => {
                tokens.push(Token::DoubleColon);
                i += 2;
            }
            b'|' if i + 1 < bytes.len() && bytes[i + 1] == b'|' => {
                tokens.push(Token::Concat);
                i += 2;
            }
            b'~' => {
                tokens.push(Token::RegexMatch);
                if i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1].is_ascii_digit() {
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                tokens.push(Token::Number(input[start..i].to_string()));
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(Token::Ident(input[start..i].to_string()));
            }
            other => {
                return Err(format!("Unexpected character '{}' in SQL", other as char));
            }
        }
    }

    Ok(tokens)
}

fn read_quoted(input: &str, idx: &mut usize, quote: u8) -> Result<String, String> {
    let bytes = input.as_bytes();
    *idx += 1; // skip opening quote
    let mut out = String::new();
    while *idx < bytes.len() {
        if bytes[*idx] == quote {
            if *idx + 1 < bytes.len() && bytes[*idx + 1] == quote {
                out.push(quote as char);
                *idx += 2;
                continue;
            }
            *idx += 1;
            return Ok(out);
        }
        // Safety on UTF-8 slice boundaries: advance by next char
        let ch = input[*idx..].chars().next().ok_or("EOF")?;
        if ch == '\\' && quote == b'\'' {
            *idx += ch.len_utf8();
            if let Some(esc) = input[*idx..].chars().next() {
                match esc {
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    other => out.push(other),
                }
                *idx += esc.len_utf8();
                continue;
            }
        }
        out.push(ch);
        *idx += ch.len_utf8();
    }
    Err("Unterminated quoted literal".into())
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Like,
    ILike,
    NotLike,
    NotILike,
    Concat,
    Add,
    Sub,
    Mul,
    Div,
    BitAnd,
    ContainsArr,
    ContainedByArr,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(CatalogValue),
    Param(usize),
    SysVar(String),
    Ident {
        table: Option<String>,
        column: String,
    },
    UnaryNot(Box<Expr>),
    UnaryNeg(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsBool {
        expr: Box<Expr>,
        expected: bool,
        negated: bool,
    },
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<CatalogQuery>,
        negated: bool,
    },
    AnyOp {
        left: Box<Expr>,
        op: BinOp,
        array: Box<Expr>,
    },
    ArrayLit(Vec<Expr>),
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        order_by: Vec<OrderItem>,
    },
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        target_type: String,
    },
    Subquery(Box<CatalogQuery>),
    Exists(Box<CatalogQuery>),
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub enum TableRef {
    Table {
        name: String,
        alias: String,
    },
    Subquery {
        query: Box<CatalogQuery>,
        alias: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Cross,
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub kind: JoinKind,
    pub target: TableRef,
    pub on: Option<Expr>,
    pub using_cols: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct CatalogSelect {
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinSpec>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct CatalogQuery {
    pub ctes: Vec<(String, CatalogQuery)>,
    pub branches: Vec<(bool, CatalogSelect)>, // bool = union_all
    pub order_by: Vec<OrderItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if self.peek_keyword(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_token(&mut self, target: &Token) -> bool {
        if self.peek() == Some(target) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_ident_raw(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Token::Ident(s)) | Some(Token::QuotedIdent(s)) => Ok(s),
            other => Err(format!("Expected identifier, found {other:?}")),
        }
    }

    pub fn parse_query(&mut self) -> Result<CatalogQuery, String> {
        let mut ctes = Vec::new();
        if self.eat_keyword("WITH") {
            let _ = self.eat_keyword("RECURSIVE");
            loop {
                let cte_name = self.expect_ident_raw()?.to_ascii_lowercase();
                if !self.eat_keyword("AS") {
                    return Err(format!("Expected AS after CTE '{cte_name}'"));
                }
                if !self.eat_token(&Token::LParen) {
                    return Err("Expected '(' around CTE query".into());
                }
                let sub = self.parse_query()?;
                if !self.eat_token(&Token::RParen) {
                    return Err("Expected ')' closing CTE query".into());
                }
                ctes.push((cte_name, sub));
                if !self.eat_token(&Token::Comma) {
                    break;
                }
            }
        }

        let mut branches = Vec::new();
        branches.push((false, self.parse_select_branch()?));
        while self.eat_keyword("UNION") {
            let all = self.eat_keyword("ALL");
            if !all {
                let _ = self.eat_keyword("DISTINCT");
            }
            branches.push((all, self.parse_select_branch()?));
        }

        let mut order_by = Vec::new();
        if self.eat_keyword("ORDER") {
            if !self.eat_keyword("BY") {
                return Err("Expected BY after ORDER".into());
            }
            order_by = self.parse_order_by()?;
        }

        let mut limit = None;
        let mut offset = None;
        loop {
            if self.eat_keyword("LIMIT") {
                if !self.eat_keyword("ALL") {
                    limit = Some(self.parse_usize()?);
                }
            } else if self.eat_keyword("OFFSET") {
                offset = Some(self.parse_usize()?);
                let _ = self.eat_keyword("ROWS");
                let _ = self.eat_keyword("ROW");
            } else {
                break;
            }
        }

        Ok(CatalogQuery {
            ctes,
            branches,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_usize(&mut self) -> Result<usize, String> {
        match self.next() {
            Some(Token::Number(n)) => n
                .parse::<usize>()
                .map_err(|_| format!("Invalid integer '{n}'")),
            Some(Token::Param(_)) => Ok(100),
            other => Err(format!("Expected integer, found {other:?}")),
        }
    }

    fn parse_select_branch(&mut self) -> Result<CatalogSelect, String> {
        let wrapped = self.eat_token(&Token::LParen);
        if !self.eat_keyword("SELECT") {
            return Err("Expected SELECT keyword".into());
        }

        let mut distinct = false;
        if self.eat_keyword("DISTINCT") {
            distinct = true;
            if self.eat_keyword("ON") && self.eat_token(&Token::LParen) {
                let mut depth = 1;
                while depth > 0 {
                    match self.next() {
                        Some(Token::LParen) => depth += 1,
                        Some(Token::RParen) => depth -= 1,
                        None => return Err("Unterminated DISTINCT ON (...)".into()),
                        _ => {}
                    }
                }
            }
        } else {
            let _ = self.eat_keyword("ALL");
        }

        let items = self.parse_select_items()?;

        let mut from = None;
        let mut joins = Vec::new();
        if self.eat_keyword("FROM") {
            from = Some(self.parse_table_ref()?);
            // Comma cross join syntax: FROM pg_class c, pg_namespace n
            loop {
                if self.eat_token(&Token::Comma) {
                    joins.push(JoinSpec {
                        kind: JoinKind::Cross,
                        target: self.parse_table_ref()?,
                        on: None,
                        using_cols: Vec::new(),
                    });
                } else if let Some(kind) = self.try_parse_join_kind() {
                    let target = self.parse_table_ref()?;
                    let mut on = None;
                    let mut using_cols = Vec::new();
                    if self.eat_keyword("ON") {
                        on = Some(self.parse_expr()?);
                    } else if self.eat_keyword("USING") {
                        if !self.eat_token(&Token::LParen) {
                            return Err("Expected '(' after USING".into());
                        }
                        loop {
                            using_cols.push(self.expect_ident_raw()?.to_ascii_lowercase());
                            if !self.eat_token(&Token::Comma) {
                                break;
                            }
                        }
                        if !self.eat_token(&Token::RParen) {
                            return Err("Expected ')' after USING".into());
                        }
                    }
                    joins.push(JoinSpec {
                        kind,
                        target,
                        on,
                        using_cols,
                    });
                } else {
                    break;
                }
            }
        }

        let where_clause = if self.eat_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.eat_keyword("GROUP") {
            if !self.eat_keyword("BY") {
                return Err("Expected BY after GROUP".into());
            }
            loop {
                group_by.push(self.parse_expr()?);
                if !self.eat_token(&Token::Comma) {
                    break;
                }
            }
        }

        if self.eat_keyword("HAVING") {
            let _ = self.parse_expr()?;
        }

        if wrapped && !self.eat_token(&Token::RParen) {
            return Err("Expected closing ')' after parenthesized SELECT".into());
        }

        Ok(CatalogSelect {
            distinct,
            items,
            from,
            joins,
            where_clause,
            group_by,
        })
    }

    fn try_parse_join_kind(&mut self) -> Option<JoinKind> {
        if self.eat_keyword("JOIN") {
            Some(JoinKind::Inner)
        } else if self.eat_keyword("INNER") {
            let _ = self.eat_keyword("JOIN");
            Some(JoinKind::Inner)
        } else if self.eat_keyword("LEFT") {
            let _ = self.eat_keyword("OUTER");
            let _ = self.eat_keyword("JOIN");
            Some(JoinKind::Left)
        } else if self.eat_keyword("RIGHT") {
            let _ = self.eat_keyword("OUTER");
            let _ = self.eat_keyword("JOIN");
            Some(JoinKind::Right)
        } else if self.eat_keyword("CROSS") {
            let _ = self.eat_keyword("JOIN");
            Some(JoinKind::Cross)
        } else {
            None
        }
    }

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>, String> {
        let mut items = Vec::new();
        loop {
            if self.eat_token(&Token::Star) {
                items.push(SelectItem::Wildcard);
            } else {
                // Check for alias.*
                let is_qual_star = matches!(
                    (
                        self.tokens.get(self.pos),
                        self.tokens.get(self.pos + 1),
                        self.tokens.get(self.pos + 2)
                    ),
                    (
                        Some(Token::Ident(_)) | Some(Token::QuotedIdent(_)),
                        Some(Token::Dot),
                        Some(Token::Star)
                    )
                );
                if is_qual_star {
                    let alias = self.expect_ident_raw()?.to_ascii_lowercase();
                    self.pos += 2; // skip . and *
                    items.push(SelectItem::QualifiedWildcard(alias));
                } else {
                    let expr = self.parse_expr()?;
                    let alias = self.parse_optional_alias()?;
                    items.push(SelectItem::Expr { expr, alias });
                }
            }
            if !self.eat_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, String> {
        if self.eat_keyword("AS") {
            return Ok(Some(self.expect_ident_raw()?));
        }
        // Implicit alias if next token is ident/quoted ident and NOT a SQL clause keyword
        match self.peek() {
            Some(Token::QuotedIdent(_)) => Ok(Some(self.expect_ident_raw()?)),
            Some(Token::Ident(s)) if !is_clause_keyword(s) => Ok(Some(self.expect_ident_raw()?)),
            _ => Ok(None),
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, String> {
        if self.eat_token(&Token::LParen) {
            let query = self.parse_query()?;
            if !self.eat_token(&Token::RParen) {
                return Err("Expected ')' after FROM subquery".into());
            }
            let alias = self
                .parse_optional_alias()?
                .unwrap_or_else(|| "subquery".into())
                .to_ascii_lowercase();
            return Ok(TableRef::Subquery {
                query: Box::new(query),
                alias,
            });
        }

        let mut parts = vec![self.expect_ident_raw()?];
        while self.eat_token(&Token::Dot) {
            parts.push(self.expect_ident_raw()?);
        }
        let full_name = normalize_ident(&parts.join("."));
        let default_alias = parts
            .last()
            .cloned()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let alias = self
            .parse_optional_alias()?
            .unwrap_or(default_alias)
            .to_ascii_lowercase();
        Ok(TableRef::Table {
            name: full_name,
            alias,
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderItem>, String> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let mut asc = true;
            if self.eat_keyword("DESC") {
                asc = false;
            } else {
                let _ = self.eat_keyword("ASC");
            }
            if self.eat_keyword("NULLS") {
                let _ = self.eat_keyword("FIRST") || self.eat_keyword("LAST");
            }
            items.push(OrderItem { expr, asc });
            if !self.eat_token(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    pub fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_and()?;
        while self.eat_keyword("OR") {
            let rhs = self.parse_and()?;
            lhs = Expr::Binary {
                left: Box::new(lhs),
                op: BinOp::Or,
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_not()?;
        while self.eat_keyword("AND") {
            let rhs = self.parse_not()?;
            lhs = Expr::Binary {
                left: Box::new(lhs),
                op: BinOp::And,
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Expr, String> {
        if self.eat_keyword("NOT") {
            return Ok(Expr::UnaryNot(Box::new(self.parse_not()?)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_concat()?;

        loop {
            if self.eat_keyword("IS") {
                let negated = self.eat_keyword("NOT");
                if self.eat_keyword("NULL") {
                    lhs = Expr::IsNull {
                        expr: Box::new(lhs),
                        negated,
                    };
                    continue;
                }
                if self.eat_keyword("TRUE") {
                    lhs = Expr::IsBool {
                        expr: Box::new(lhs),
                        expected: true,
                        negated,
                    };
                    continue;
                }
                if self.eat_keyword("FALSE") {
                    lhs = Expr::IsBool {
                        expr: Box::new(lhs),
                        expected: false,
                        negated,
                    };
                    continue;
                }
                if self.eat_keyword("DISTINCT") && self.eat_keyword("FROM") {
                    let rhs = self.parse_concat()?;
                    lhs = Expr::Binary {
                        left: Box::new(lhs),
                        op: if negated { BinOp::Eq } else { BinOp::Neq },
                        right: Box::new(rhs),
                    };
                    continue;
                }
            }

            let negated_prefix = self.eat_keyword("NOT");
            if self.eat_keyword("IN") {
                if !self.eat_token(&Token::LParen) {
                    return Err("Expected '(' after IN".into());
                }
                if self.peek_keyword("SELECT") || self.peek_keyword("WITH") {
                    let sub = self.parse_query()?;
                    if !self.eat_token(&Token::RParen) {
                        return Err("Expected ')' after IN subquery".into());
                    }
                    lhs = Expr::InSubquery {
                        expr: Box::new(lhs),
                        subquery: Box::new(sub),
                        negated: negated_prefix,
                    };
                } else {
                    let mut list = Vec::new();
                    if !self.eat_token(&Token::RParen) {
                        loop {
                            list.push(self.parse_expr()?);
                            if !self.eat_token(&Token::Comma) {
                                break;
                            }
                        }
                        if !self.eat_token(&Token::RParen) {
                            return Err("Expected ')' after IN list".into());
                        }
                    }
                    lhs = Expr::InList {
                        expr: Box::new(lhs),
                        list,
                        negated: negated_prefix,
                    };
                }
                continue;
            }

            if self.eat_keyword("BETWEEN") {
                let low = self.parse_concat()?;
                if !self.eat_keyword("AND") {
                    return Err("Expected AND in BETWEEN".into());
                }
                let high = self.parse_concat()?;
                lhs = Expr::Between {
                    expr: Box::new(lhs),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: negated_prefix,
                };
                continue;
            }

            if self.eat_keyword("ILIKE") {
                let rhs = self.parse_concat()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: if negated_prefix {
                        BinOp::NotILike
                    } else {
                        BinOp::ILike
                    },
                    right: Box::new(rhs),
                };
                continue;
            }

            if self.eat_keyword("LIKE") {
                let rhs = self.parse_concat()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: if negated_prefix {
                        BinOp::NotLike
                    } else {
                        BinOp::Like
                    },
                    right: Box::new(rhs),
                };
                continue;
            }

            if negated_prefix {
                return Err("Unexpected NOT in expression".into());
            }

            let op = match self.peek() {
                Some(Token::Eq) => Some(BinOp::Eq),
                Some(Token::Neq) => Some(BinOp::Neq),
                Some(Token::Lt) => Some(BinOp::Lt),
                Some(Token::Lte) => Some(BinOp::Lte),
                Some(Token::Gt) => Some(BinOp::Gt),
                Some(Token::Gte) => Some(BinOp::Gte),
                Some(Token::ContainsArr) => Some(BinOp::ContainsArr),
                Some(Token::ContainedByArr) => Some(BinOp::ContainedByArr),
                Some(Token::RegexMatch) => Some(BinOp::Regex),
                Some(Token::RegexNotMatch) => Some(BinOp::NotRegex),
                _ => None,
            };

            if let Some(op) = op {
                self.pos += 1;
                if self.eat_keyword("ANY") || self.eat_keyword("SOME") {
                    if !self.eat_token(&Token::LParen) {
                        return Err("Expected '(' after ANY".into());
                    }
                    let array_expr = self.parse_expr()?;
                    if !self.eat_token(&Token::RParen) {
                        return Err("Expected ')' after ANY(...)".into());
                    }
                    lhs = Expr::AnyOp {
                        left: Box::new(lhs),
                        op,
                        array: Box::new(array_expr),
                    };
                } else {
                    let rhs = self.parse_concat()?;
                    lhs = Expr::Binary {
                        left: Box::new(lhs),
                        op,
                        right: Box::new(rhs),
                    };
                }
                continue;
            }

            break;
        }

        Ok(lhs)
    }

    fn parse_concat(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_add()?;
        while self.eat_token(&Token::Concat) {
            let rhs = self.parse_add()?;
            lhs = Expr::Binary {
                left: Box::new(lhs),
                op: BinOp::Concat,
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_add(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_mul()?;
        loop {
            if self.eat_token(&Token::Plus) {
                let rhs = self.parse_mul()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: BinOp::Add,
                    right: Box::new(rhs),
                };
            } else if self.eat_token(&Token::Minus) {
                let rhs = self.parse_mul()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: BinOp::Sub,
                    right: Box::new(rhs),
                };
            } else {
                break;
            }
        }
        Ok(lhs)
    }

    fn parse_mul(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_postfix()?;
        loop {
            if self.eat_token(&Token::Star) {
                let rhs = self.parse_postfix()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: BinOp::Mul,
                    right: Box::new(rhs),
                };
            } else if self.eat_token(&Token::Slash) {
                let rhs = self.parse_postfix()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: BinOp::Div,
                    right: Box::new(rhs),
                };
            } else if self.eat_token(&Token::Amp) {
                let rhs = self.parse_postfix()?;
                lhs = Expr::Binary {
                    left: Box::new(lhs),
                    op: BinOp::BitAnd,
                    right: Box::new(rhs),
                };
            } else {
                break;
            }
        }
        Ok(lhs)
    }

    fn parse_postfix(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.eat_token(&Token::DoubleColon) {
                let target_type = self.parse_type_name()?;
                expr = Expr::Cast {
                    expr: Box::new(expr),
                    target_type,
                };
            } else if self.eat_token(&Token::LBracket) {
                // Array subscript expr[idx] (consume cleanly)
                let _idx = self.parse_expr()?;
                let _ = self.eat_token(&Token::RBracket);
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_type_name(&mut self) -> Result<String, String> {
        let mut name = self.expect_ident_raw()?;
        if self.eat_token(&Token::Dot) {
            let right = self.expect_ident_raw()?;
            name = right;
        }
        if self.peek_keyword("VARYING") || self.peek_keyword("PRECISION") {
            let ext = self.expect_ident_raw()?;
            name = format!("{name} {ext}");
        }
        if self.eat_token(&Token::LParen) {
            let mut depth = 1;
            while depth > 0 {
                match self.next() {
                    Some(Token::LParen) => depth += 1,
                    Some(Token::RParen) => depth -= 1,
                    None => break,
                    _ => {}
                }
            }
        }
        if self.eat_token(&Token::LBracket) {
            let _ = self.eat_token(&Token::RBracket);
            name.push_str("[]");
        }
        Ok(name.to_ascii_lowercase())
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        if self.eat_token(&Token::Minus) {
            return Ok(Expr::UnaryNeg(Box::new(self.parse_postfix()?)));
        }

        if self.eat_keyword("NULL") {
            return Ok(Expr::Literal(CatalogValue::Null));
        }
        if self.eat_keyword("TRUE") {
            return Ok(Expr::Literal(CatalogValue::Bool(true)));
        }
        if self.eat_keyword("FALSE") {
            return Ok(Expr::Literal(CatalogValue::Bool(false)));
        }

        if self.eat_keyword("BINARY") {
            return self.parse_primary();
        }

        if self.eat_keyword("EXISTS") {
            if !self.eat_token(&Token::LParen) {
                return Err("Expected '(' after EXISTS".into());
            }
            let sub = self.parse_query()?;
            if !self.eat_token(&Token::RParen) {
                return Err("Expected ')' after EXISTS".into());
            }
            return Ok(Expr::Exists(Box::new(sub)));
        }

        if self.eat_keyword("CASE") {
            let operand = if !self.peek_keyword("WHEN") {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            let mut whens = Vec::new();
            while self.eat_keyword("WHEN") {
                let cond = self.parse_expr()?;
                if !self.eat_keyword("THEN") {
                    return Err("Expected THEN in CASE expression".into());
                }
                let val = self.parse_expr()?;
                whens.push((cond, val));
            }
            let else_expr = if self.eat_keyword("ELSE") {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            if !self.eat_keyword("END") {
                return Err("Expected END closing CASE".into());
            }
            return Ok(Expr::Case {
                operand,
                whens,
                else_expr,
            });
        }

        if self.eat_keyword("CAST") {
            if !self.eat_token(&Token::LParen) {
                return Err("Expected '(' after CAST".into());
            }
            let inner = self.parse_expr()?;
            if !self.eat_keyword("AS") {
                return Err("Expected AS inside CAST".into());
            }
            let target_type = self.parse_type_name()?;
            if !self.eat_token(&Token::RParen) {
                return Err("Expected ')' after CAST".into());
            }
            return Ok(Expr::Cast {
                expr: Box::new(inner),
                target_type,
            });
        }

        if self.eat_keyword("ARRAY") {
            if self.eat_token(&Token::LBracket) {
                let mut elems = Vec::new();
                if !self.eat_token(&Token::RBracket) {
                    loop {
                        elems.push(self.parse_expr()?);
                        if !self.eat_token(&Token::Comma) {
                            break;
                        }
                    }
                    if !self.eat_token(&Token::RBracket) {
                        return Err("Expected ']' closing ARRAY[...]".into());
                    }
                }
                return Ok(Expr::ArrayLit(elems));
            }
            if self.eat_token(&Token::LParen) {
                let sub = self.parse_query()?;
                if !self.eat_token(&Token::RParen) {
                    return Err("Expected ')' closing ARRAY(...)".into());
                }
                return Ok(Expr::Subquery(Box::new(sub)));
            }
        }

        if self.eat_token(&Token::LParen) {
            if self.peek_keyword("SELECT") || self.peek_keyword("WITH") {
                let sub = self.parse_query()?;
                if !self.eat_token(&Token::RParen) {
                    return Err("Expected ')' closing scalar subquery".into());
                }
                return Ok(Expr::Subquery(Box::new(sub)));
            }
            let inner = self.parse_expr()?;
            if !self.eat_token(&Token::RParen) {
                return Err("Expected closing ')'".into());
            }
            return Ok(inner);
        }

        match self.next() {
            Some(Token::StringLit(s)) => Ok(Expr::Literal(CatalogValue::Text(s))),
            Some(Token::Number(n)) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expr::Literal(CatalogValue::Int(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expr::Literal(CatalogValue::Float(f)))
                } else {
                    Ok(Expr::Literal(CatalogValue::Text(n)))
                }
            }
            Some(Token::Param(idx)) => Ok(Expr::Param(idx)),
            Some(Token::SysVar(sv)) => Ok(Expr::SysVar(sv)),
            Some(Token::Ident(first)) | Some(Token::QuotedIdent(first)) => {
                let mut parts = vec![first];
                while self.eat_token(&Token::Dot) {
                    if self.eat_token(&Token::Star) {
                        parts.push("*".into());
                        break;
                    }
                    parts.push(self.expect_ident_raw()?);
                }

                // Function call?
                if self.eat_token(&Token::LParen) {
                    let fn_name = parts
                        .last()
                        .cloned()
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let distinct = self.eat_keyword("DISTINCT");
                    let mut args = Vec::new();
                    let mut order_by = Vec::new();
                    if !self.eat_token(&Token::RParen) {
                        if self.eat_token(&Token::Star) {
                            let _ = self.eat_token(&Token::RParen);
                            args.push(Expr::Literal(CatalogValue::Int(1)));
                        } else {
                            loop {
                                args.push(self.parse_expr()?);
                                if !self.eat_token(&Token::Comma) {
                                    break;
                                }
                            }
                            if self.eat_keyword("ORDER") {
                                if !self.eat_keyword("BY") {
                                    return Err("Expected BY after ORDER inside aggregate".into());
                                }
                                order_by = self.parse_order_by()?;
                            }
                            if !self.eat_token(&Token::RParen) {
                                return Err(format!("Expected ')' closing {fn_name}(...)"));
                            }
                        }
                    }
                    // Ignore optional `FILTER (...)` / `OVER (...)` clauses after aggregate calls
                    if self.eat_keyword("FILTER") && self.eat_token(&Token::LParen) {
                        skip_balanced_parens(self);
                    }
                    if self.eat_keyword("OVER") && self.eat_token(&Token::LParen) {
                        skip_balanced_parens(self);
                    }
                    return Ok(Expr::Function {
                        name: fn_name,
                        args,
                        distinct,
                        order_by,
                    });
                }

                if parts.len() == 1 {
                    Ok(Expr::Ident {
                        table: None,
                        column: parts[0].to_ascii_lowercase(),
                    })
                } else {
                    let col = parts.pop().unwrap_or_default().to_ascii_lowercase();
                    let tbl = parts.pop().unwrap_or_default().to_ascii_lowercase();
                    Ok(Expr::Ident {
                        table: Some(tbl),
                        column: col,
                    })
                }
            }
            other => Err(format!("Unexpected token in SQL expression: {other:?}")),
        }
    }
}

fn skip_balanced_parens(parser: &mut Parser) {
    let mut depth = 1usize;
    while depth > 0 {
        match parser.next() {
            Some(Token::LParen) => depth += 1,
            Some(Token::RParen) => depth -= 1,
            None => break,
            _ => {}
        }
    }
}

fn is_clause_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "FROM"
            | "WHERE"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "CROSS"
            | "OUTER"
            | "ON"
            | "USING"
            | "GROUP"
            | "HAVING"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "UNION"
            | "ALL"
            | "AS"
            | "AND"
            | "OR"
            | "NOT"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
    )
}

/// Inspect a parsed query to determine whether ALL non-CTE referenced relations are virtual catalogs
/// (or whether it is a catalog/bootstrap scalar query without a FROM clause).
pub fn is_virtual_catalog_query(query: &CatalogQuery) -> bool {
    let cte_names: std::collections::HashSet<String> =
        query.ctes.iter().map(|(n, _)| n.clone()).collect();
    let mut table_names = Vec::new();
    collect_table_names(query, &cte_names, &mut table_names);

    if !table_names.is_empty() {
        return table_names
            .iter()
            .all(|t| VirtualCatalog::is_catalog_table_name(t));
    }

    // Scalar query without FROM (only route here if it invokes catalog/bootstrap functions, sysvars, or constants)
    query
        .branches
        .iter()
        .all(|(_, b)| b.from.is_none() && b.items.iter().all(item_is_scalar_catalog_expr))
}

fn collect_table_names(
    query: &CatalogQuery,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<String>,
) {
    for (_, cte_q) in &query.ctes {
        collect_table_names(cte_q, cte_names, out);
    }
    for (_, b) in &query.branches {
        if let Some(ref f) = b.from {
            collect_from_table(f, cte_names, out);
        }
        for j in &b.joins {
            collect_from_table(&j.target, cte_names, out);
        }
        for item in &b.items {
            if let SelectItem::Expr { expr, .. } = item {
                collect_expr_tables(expr, cte_names, out);
            }
        }
        if let Some(ref w) = b.where_clause {
            collect_expr_tables(w, cte_names, out);
        }
    }
}

fn collect_from_table(
    tbl: &TableRef,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<String>,
) {
    match tbl {
        TableRef::Table { name, .. } => {
            if !cte_names.contains(name) {
                out.push(name.clone());
            }
        }
        TableRef::Subquery { query, .. } => collect_table_names(query, cte_names, out),
    }
}

fn collect_expr_tables(
    expr: &Expr,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<String>,
) {
    match expr {
        Expr::Subquery(q) | Expr::Exists(q) | Expr::InSubquery { subquery: q, .. } => {
            collect_table_names(q, cte_names, out);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_tables(left, cte_names, out);
            collect_expr_tables(right, cte_names, out);
        }
        Expr::Function { args, .. } => {
            for a in args {
                collect_expr_tables(a, cte_names, out);
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_expr_tables(op, cte_names, out);
            }
            for (c, v) in whens {
                collect_expr_tables(c, cte_names, out);
                collect_expr_tables(v, cte_names, out);
            }
            if let Some(e) = else_expr {
                collect_expr_tables(e, cte_names, out);
            }
        }
        _ => {}
    }
}

fn item_is_scalar_catalog_expr(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_is_scalar_catalog(expr),
        _ => false,
    }
}

fn expr_is_scalar_catalog(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::SysVar(_) => true,
        Expr::Cast { expr, .. } => expr_is_scalar_catalog(expr),
        Expr::Ident {
            table: None,
            column,
        } => matches!(
            column.as_str(),
            "current_user" | "session_user" | "user" | "current_schema" | "current_database"
        ),
        Expr::Function { .. } => true,
        Expr::Subquery(q) => is_virtual_catalog_query(q),
        _ => false,
    }
}
