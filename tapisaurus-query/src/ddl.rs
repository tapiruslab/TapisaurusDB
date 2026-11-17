//! SQL DDL schema extractor for `CREATE TABLE` and `ALTER TABLE`.
//!
//! Parses column data types, `NOT NULL`, `DEFAULT`, `PRIMARY KEY`, `UNIQUE`, and
//! `FOREIGN KEY ... REFERENCES ...` constraints into [`RelationalSchema`] metadata so
//! `tapisaurus-catalog` accurately exposes relational DDL to ORMs and GUI tools.

use tapisaurus_core::{ColumnSchema, ForeignKeyDef, RelationalSchema};

/// Normalize a possibly quoted/schema-qualified SQL identifier (`"public"."users"` -> `"users"`).
pub fn normalize_table_ident(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    if let Some((_schema, tbl)) = split_schema_qualified(trimmed) {
        strip_quotes(tbl)
    } else {
        strip_quotes(trimmed)
    }
}

fn split_schema_qualified(s: &str) -> Option<(&str, &str)> {
    let mut in_sq = false;
    let mut in_dq = false;
    let mut in_bt = false;
    let mut brace_depth = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '\'' if !in_dq && !in_bt => in_sq = !in_sq,
            '"' if !in_sq && !in_bt => in_dq = !in_dq,
            '`' if !in_sq && !in_dq => in_bt = !in_bt,
            '{' if !in_sq && !in_dq && !in_bt => brace_depth += 1,
            '}' if !in_sq && !in_dq && !in_bt => brace_depth = brace_depth.saturating_sub(1),
            '.' if !in_sq && !in_dq && !in_bt && brace_depth == 0 => {
                return Some((&s[..i], &s[i + 1..]));
            }
            _ => {}
        }
    }
    None
}

fn strip_quotes(s: &str) -> String {
    s.trim()
        .trim_matches(|c| c == '"' || c == '`' || c == '\'')
        .to_string()
}

/// Split top-level comma-separated clauses inside `CREATE TABLE (...)`.
fn split_top_level_commas(body: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut in_bt = false;

    for (i, ch) in body.char_indices() {
        match ch {
            '\'' if !in_dq && !in_bt => in_sq = !in_sq,
            '"' if !in_sq && !in_bt => in_dq = !in_dq,
            '`' if !in_sq && !in_dq => in_bt = !in_bt,
            '(' if !in_sq && !in_dq && !in_bt => depth += 1,
            ')' if !in_sq && !in_dq && !in_bt => depth = depth.saturating_sub(1),
            ',' if !in_sq && !in_dq && !in_bt && depth == 0 => {
                let slice = body[start..i].trim();
                if !slice.is_empty() {
                    out.push(slice);
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    let tail = body[start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out
}

/// Find matching closing `)` for the opening `(` at `open_idx`.
fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut in_bt = false;
    for (i, ch) in s[open_idx..].char_indices() {
        match ch {
            '\'' if !in_dq && !in_bt => in_sq = !in_sq,
            '"' if !in_sq && !in_bt => in_dq = !in_dq,
            '`' if !in_sq && !in_dq => in_bt = !in_bt,
            '(' if !in_sq && !in_dq && !in_bt => depth += 1,
            ')' if !in_sq && !in_dq && !in_bt => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(open_idx + i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split DDL item tokens keeping parenthesized type parameters and quoted literals together.
fn split_ddl_tokens(clause: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut in_bt = false;

    for ch in clause.chars() {
        match ch {
            '\'' if !in_dq && !in_bt => {
                in_sq = !in_sq;
                cur.push(ch);
            }
            '"' if !in_sq && !in_bt => {
                in_dq = !in_dq;
                cur.push(ch);
            }
            '`' if !in_sq && !in_dq => {
                in_bt = !in_bt;
                cur.push(ch);
            }
            '(' if !in_sq && !in_dq && !in_bt => {
                depth += 1;
                cur.push(ch);
            }
            ')' if !in_sq && !in_dq && !in_bt => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
                if depth == 0 && !cur.trim().is_empty() {
                    tokens.push(cur.trim().to_string());
                    cur.clear();
                }
            }
            c if c.is_whitespace() && !in_sq && !in_dq && !in_bt && depth == 0 => {
                if !cur.trim().is_empty() {
                    tokens.push(cur.trim().to_string());
                    cur.clear();
                }
            }
            other => cur.push(other),
        }
    }
    if !cur.trim().is_empty() {
        tokens.push(cur.trim().to_string());
    }
    tokens
}

fn parse_paren_ident_list(token: &str) -> Vec<String> {
    let inner = token.trim().trim_start_matches('(').trim_end_matches(')');
    inner
        .split(',')
        .map(strip_quotes)
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_fk_actions(tokens: &[String], start: usize) -> (String, String) {
    let mut on_delete = "NO ACTION".to_string();
    let mut on_update = "NO ACTION".to_string();
    let mut i = start;
    while i + 2 < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("ON") {
            let target = tokens[i + 1].to_ascii_uppercase();
            let mut action = tokens[i + 2].to_ascii_uppercase();
            let mut step = 3;
            if i + 3 < tokens.len()
                && ((action == "NO" && tokens[i + 3].eq_ignore_ascii_case("ACTION"))
                    || (action == "SET"
                        && (tokens[i + 3].eq_ignore_ascii_case("NULL")
                            || tokens[i + 3].eq_ignore_ascii_case("DEFAULT"))))
            {
                action = format!("{action} {}", tokens[i + 3].to_ascii_uppercase());
                step = 4;
            }
            if target == "DELETE" {
                on_delete = action;
            } else if target == "UPDATE" {
                on_update = action;
            }
            i += step;
        } else {
            i += 1;
        }
    }
    (on_delete, on_update)
}

fn parse_references_clause(
    tokens: &[String],
    ref_idx: usize,
) -> Option<(String, String, String, String)> {
    let tbl_tok = tokens.get(ref_idx + 1)?;
    let (ref_tbl_raw, inline_col) = if let Some(p_idx) = tbl_tok.find('(') {
        (
            &tbl_tok[..p_idx],
            Some(parse_paren_ident_list(&tbl_tok[p_idx..])),
        )
    } else {
        (tbl_tok.as_str(), None)
    };
    let ref_table = normalize_table_ident(ref_tbl_raw);
    let (ref_col, actions_start) = if let Some(cols) = inline_col {
        (
            cols.into_iter().next().unwrap_or_else(|| "id".into()),
            ref_idx + 2,
        )
    } else if let Some(col_tok) = tokens.get(ref_idx + 2) {
        if col_tok.starts_with('(') {
            let cols = parse_paren_ident_list(col_tok);
            (
                cols.into_iter().next().unwrap_or_else(|| "id".into()),
                ref_idx + 3,
            )
        } else {
            ("id".to_string(), ref_idx + 2)
        }
    } else {
        ("id".to_string(), ref_idx + 2)
    };
    let (on_delete, on_update) = parse_fk_actions(tokens, actions_start);
    Some((ref_table, ref_col, on_delete, on_update))
}

/// Extract `(table_name, Option<RelationalSchema>)` from `CREATE TABLE ...`.
pub fn parse_create_table_ddl(input: &str) -> Option<(String, Option<RelationalSchema>)> {
    let trimmed = input.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE TABLE ") {
        return None;
    }

    let mut rest = trimmed[13..].trim();
    if rest.to_ascii_uppercase().starts_with("IF NOT EXISTS ") {
        rest = rest[14..].trim();
    }

    let Some(open_idx) = rest.find('(') else {
        let name = normalize_table_ident(rest);
        return (!name.is_empty()).then_some((name, None));
    };

    let table_name = normalize_table_ident(&rest[..open_idx]);
    if table_name.is_empty() {
        return None;
    }

    let close_idx = find_matching_paren(rest, open_idx)?;
    let body = &rest[open_idx + 1..close_idx];
    let items = split_top_level_commas(body);

    let mut schema = RelationalSchema::default();

    for raw_item in items {
        let toks = split_ddl_tokens(raw_item);
        if toks.is_empty() {
            continue;
        }
        let first_upper = toks[0].to_ascii_uppercase();

        // Check table-level constraints (`CONSTRAINT ...`, `PRIMARY KEY (...)`, `FOREIGN KEY (...)`, `UNIQUE (...)`)
        if matches!(
            first_upper.as_str(),
            "CONSTRAINT" | "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "INDEX" | "KEY"
        ) {
            let (explicit_name, kw_idx) = if first_upper == "CONSTRAINT" && toks.len() >= 3 {
                (Some(strip_quotes(&toks[1])), 2usize)
            } else {
                (None, 0usize)
            };
            let kw = toks
                .get(kw_idx)
                .map(|s| s.to_ascii_uppercase())
                .unwrap_or_default();

            if kw == "PRIMARY"
                && toks
                    .get(kw_idx + 1)
                    .is_some_and(|s| s.eq_ignore_ascii_case("KEY"))
            {
                if let Some(col_tok) = toks.get(kw_idx + 2) {
                    let pk_cols = parse_paren_ident_list(col_tok);
                    for pk in &pk_cols {
                        if let Some(c) = schema.columns.iter_mut().find(|c| &c.name == pk) {
                            c.is_primary_key = true;
                            c.nullable = false;
                        }
                    }
                    schema.primary_key = pk_cols;
                }
            } else if kw == "UNIQUE" {
                let cols_idx = if toks.get(kw_idx + 1).is_some_and(|s| {
                    s.eq_ignore_ascii_case("KEY") || s.eq_ignore_ascii_case("INDEX")
                }) {
                    kw_idx + 2
                } else {
                    kw_idx + 1
                };
                if let Some(col_tok) = toks.get(cols_idx) {
                    let uq_cols = if col_tok.starts_with('(') {
                        parse_paren_ident_list(col_tok)
                    } else if let Some(next_tok) = toks.get(cols_idx + 1) {
                        parse_paren_ident_list(next_tok)
                    } else {
                        Vec::new()
                    };
                    if uq_cols.len() == 1 {
                        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == uq_cols[0]) {
                            c.is_unique = true;
                        }
                    }
                }
            } else if kw == "FOREIGN"
                && toks
                    .get(kw_idx + 1)
                    .is_some_and(|s| s.eq_ignore_ascii_case("KEY"))
            {
                let local_col = toks
                    .get(kw_idx + 2)
                    .map(|t| parse_paren_ident_list(t))
                    .unwrap_or_default()
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                if let Some(ref_pos) = toks
                    .iter()
                    .position(|t| t.eq_ignore_ascii_case("REFERENCES"))
                {
                    if let Some((ref_table, ref_col, on_delete, on_update)) =
                        parse_references_clause(&toks, ref_pos)
                    {
                        let cname = explicit_name
                            .unwrap_or_else(|| format!("{table_name}_{local_col}_fkey"));
                        schema.add_foreign_key(ForeignKeyDef {
                            constraint_name: cname,
                            column: local_col,
                            referenced_table: ref_table,
                            referenced_column: ref_col,
                            on_delete,
                            on_update,
                        });
                    }
                }
            }
            continue;
        }

        // Column definition: `<col_name> <type...> [constraints...]`
        let col_name = strip_quotes(&toks[0]);
        if col_name.is_empty() || toks.len() < 2 {
            continue;
        }

        let (raw_type, mut idx) = collect_sql_type_tokens(&toks[1..]);
        idx += 1; // offset relative to toks

        let mut nullable = true;
        let mut is_pk = false;
        let mut is_unique = false;
        let mut default_expr = None;

        while idx < toks.len() {
            let cur_u = toks[idx].to_ascii_uppercase();
            if cur_u == "NOT"
                && toks
                    .get(idx + 1)
                    .is_some_and(|s| s.eq_ignore_ascii_case("NULL"))
            {
                nullable = false;
                idx += 2;
            } else if cur_u == "PRIMARY"
                && toks
                    .get(idx + 1)
                    .is_some_and(|s| s.eq_ignore_ascii_case("KEY"))
            {
                is_pk = true;
                nullable = false;
                idx += 2;
            } else if cur_u == "UNIQUE" {
                is_unique = true;
                idx += 1;
            } else if cur_u == "DEFAULT" {
                if let Some(def_tok) = toks.get(idx + 1) {
                    default_expr = Some(def_tok.clone());
                    idx += 2;
                } else {
                    idx += 1;
                }
            } else if cur_u == "REFERENCES" {
                if let Some((ref_table, ref_col, on_delete, on_update)) =
                    parse_references_clause(&toks, idx)
                {
                    schema.add_foreign_key(ForeignKeyDef {
                        constraint_name: format!("{table_name}_{col_name}_fkey"),
                        column: col_name.clone(),
                        referenced_table: ref_table,
                        referenced_column: ref_col,
                        on_delete,
                        on_update,
                    });
                }
                break;
            } else {
                idx += 1;
            }
        }

        if is_pk && !schema.primary_key.contains(&col_name) {
            schema.primary_key.push(col_name.clone());
        }
        schema.add_column(ColumnSchema::from_sql_type(
            col_name,
            &raw_type,
            nullable,
            default_expr,
            is_pk,
            is_unique,
        ));
    }

    Some((table_name, Some(schema)))
}

fn collect_sql_type_tokens(after_name: &[String]) -> (String, usize) {
    if after_name.is_empty() {
        return ("text".to_string(), 0);
    }
    let first_u = after_name[0].to_ascii_uppercase();
    // Multi-word SQL types: DOUBLE PRECISION, CHARACTER VARYING[(n)], TIMESTAMP WITH/WITHOUT TIME ZONE
    if first_u == "DOUBLE"
        && after_name
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case("PRECISION"))
    {
        return ("double precision".to_string(), 2);
    }
    if first_u == "CHARACTER"
        && after_name
            .get(1)
            .is_some_and(|s| s.to_ascii_uppercase().starts_with("VARYING"))
    {
        return (format!("character {}", after_name[1]), 2);
    }
    if (first_u == "TIMESTAMP" || first_u == "TIME")
        && after_name
            .get(2)
            .is_some_and(|s| s.eq_ignore_ascii_case("TIME"))
        && after_name
            .get(3)
            .is_some_and(|s| s.eq_ignore_ascii_case("ZONE"))
    {
        let full = after_name[..4].join(" ");
        return (full, 4);
    }
    if after_name
        .get(1)
        .is_some_and(|s| s.starts_with('(') && s.ends_with(')'))
    {
        return (format!("{}{}", after_name[0], after_name[1]), 2);
    }
    (after_name[0].clone(), 1)
}

/// Extract a `FOREIGN KEY` definition from `ALTER TABLE t ADD [CONSTRAINT name] FOREIGN KEY (col) REFERENCES ref(ref_col) ...`.
pub fn parse_alter_add_foreign_key(table_name: &str, after_name: &str) -> Option<ForeignKeyDef> {
    let toks = split_ddl_tokens(after_name);
    let fk_pos = toks
        .windows(2)
        .position(|w| w[0].eq_ignore_ascii_case("FOREIGN") && w[1].eq_ignore_ascii_case("KEY"))?;

    let constraint_name = if let Some(c_pos) = toks
        .iter()
        .position(|t| t.eq_ignore_ascii_case("CONSTRAINT"))
    {
        toks.get(c_pos + 1).map(|s| strip_quotes(s))
    } else {
        None
    };
    let local_col = toks
        .get(fk_pos + 2)
        .map(|t| parse_paren_ident_list(t))
        .unwrap_or_default()
        .into_iter()
        .next()?;
    let ref_pos = toks
        .iter()
        .position(|t| t.eq_ignore_ascii_case("REFERENCES"))?;
    let (ref_table, ref_col, on_delete, on_update) = parse_references_clause(&toks, ref_pos)?;
    let norm_tbl = normalize_table_ident(table_name);
    Some(ForeignKeyDef {
        constraint_name: constraint_name.unwrap_or_else(|| format!("{norm_tbl}_{local_col}_fkey")),
        column: local_col,
        referenced_table: ref_table,
        referenced_column: ref_col,
        on_delete,
        on_update,
    })
}
