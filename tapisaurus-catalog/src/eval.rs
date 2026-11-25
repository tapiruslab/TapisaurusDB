//! Virtual System Catalog Query Evaluator & Schema Describer.

use std::collections::{BTreeMap, HashSet};

use crate::materializer::format_pg_type;
use crate::parser::{
    is_virtual_catalog_query, tokenize, BinOp, CatalogQuery, CatalogSelect, Expr, JoinKind, Parser,
    SelectItem, TableRef,
};
use crate::types::{
    normalize_ident, CatalogColumn, CatalogDialect, CatalogResult, CatalogTable, CatalogValue,
    VirtualCatalog,
};

/// Bound row across joined relations (`alias -> table`) plus optional correlated outer scope.
#[derive(Debug, Clone)]
struct BoundRow {
    sources: Vec<(String, Vec<(CatalogColumn, CatalogValue)>)>,
}

impl BoundRow {
    fn empty() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    fn from_table(alias: String, cols: &[CatalogColumn], vals: &[CatalogValue]) -> Self {
        let pairs = cols.iter().cloned().zip(vals.iter().cloned()).collect();
        Self {
            sources: vec![(alias, pairs)],
        }
    }

    fn combine(&self, other: &BoundRow) -> Self {
        let mut sources = self.sources.clone();
        sources.extend(other.sources.iter().cloned());
        Self { sources }
    }

    fn combine_nulls(&self, alias: String, cols: &[CatalogColumn]) -> Self {
        let pairs = cols
            .iter()
            .cloned()
            .map(|c| (c, CatalogValue::Null))
            .collect();
        let mut sources = self.sources.clone();
        sources.push((alias, pairs));
        Self { sources }
    }

    fn lookup(&self, table: Option<&str>, col: &str) -> Option<&CatalogValue> {
        if let Some(tbl) = table {
            let tbl_l = tbl.to_ascii_lowercase();
            for (alias, pairs) in &self.sources {
                if alias == &tbl_l {
                    for (c, v) in pairs {
                        if c.name.eq_ignore_ascii_case(col) {
                            return Some(v);
                        }
                    }
                }
            }
            None
        } else {
            for (_, pairs) in &self.sources {
                for (c, v) in pairs {
                    if c.name.eq_ignore_ascii_case(col) {
                        return Some(v);
                    }
                }
            }
            None
        }
    }
}

#[derive(Debug, Clone)]
struct SchemaEntry {
    alias: String,
    columns: Vec<CatalogColumn>,
}

/// Try executing `sql` against `catalog`. Returns `Ok(None)` if the query targets user collections
/// rather than virtual system catalogs.
pub fn try_execute_catalog_query(
    catalog: &VirtualCatalog,
    sql: &str,
) -> Result<Option<CatalogResult>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let tokens = tokenize(trimmed)?;
    if tokens.is_empty() {
        return Ok(None);
    }

    let mut parser = Parser::new(tokens);
    let query = match parser.parse_query() {
        Ok(q) => q,
        Err(_) => return Ok(None),
    };

    if !is_virtual_catalog_query(&query) {
        return Ok(None);
    }

    let table = eval_query(catalog, &query, &BTreeMap::new(), None)?;
    let rows = table
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(Some).collect())
        .collect();
    Ok(Some(CatalogResult {
        columns: table.columns,
        rows,
    }))
}

/// Infer output column metadata (`RowDescription`) for Extended Query `'D'` (Describe)
/// BEFORE parameter values are bound.
pub fn try_describe_catalog_query(
    catalog: &VirtualCatalog,
    sql: &str,
) -> Result<Option<Vec<CatalogColumn>>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let tokens = tokenize(trimmed)?;
    if tokens.is_empty() {
        return Ok(None);
    }

    let mut parser = Parser::new(tokens);
    let query = match parser.parse_query() {
        Ok(q) => q,
        Err(_) => return Ok(None),
    };

    if !is_virtual_catalog_query(&query) {
        return Ok(None);
    }

    let mut ctes = BTreeMap::new();
    for (name, cte_q) in &query.ctes {
        let cols = describe_query(catalog, cte_q, &ctes)?;
        ctes.insert(
            name.clone(),
            CatalogTable {
                schema: String::new(),
                name: name.clone(),
                columns: cols,
                rows: Vec::new(),
            },
        );
    }
    let cols = describe_query(catalog, &query, &ctes)?;
    Ok(Some(cols))
}

fn describe_query(
    catalog: &VirtualCatalog,
    query: &CatalogQuery,
    outer_ctes: &BTreeMap<String, CatalogTable>,
) -> Result<Vec<CatalogColumn>, String> {
    let (_, branch) = query
        .branches
        .first()
        .ok_or("Empty SELECT query in catalog engine")?;
    let schemas = collect_branch_schemas(catalog, branch, outer_ctes)?;
    let mut cols = Vec::new();
    for item in &branch.items {
        match item {
            SelectItem::Wildcard => {
                for s in &schemas {
                    cols.extend(s.columns.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(alias) => {
                if let Some(s) = schemas.iter().find(|s| s.alias == *alias) {
                    cols.extend(s.columns.iter().cloned());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| default_col_name(expr));
                let oid = infer_expr_pg_oid(expr, &schemas);
                cols.push(CatalogColumn::new(name, oid));
            }
        }
    }
    Ok(cols)
}

fn collect_branch_schemas(
    catalog: &VirtualCatalog,
    branch: &CatalogSelect,
    ctes: &BTreeMap<String, CatalogTable>,
) -> Result<Vec<SchemaEntry>, String> {
    let mut schemas = Vec::new();
    if let Some(ref f) = branch.from {
        schemas.push(describe_table_ref(catalog, f, ctes)?);
    }
    for j in &branch.joins {
        schemas.push(describe_table_ref(catalog, &j.target, ctes)?);
    }
    Ok(schemas)
}

fn describe_table_ref(
    catalog: &VirtualCatalog,
    tref: &TableRef,
    ctes: &BTreeMap<String, CatalogTable>,
) -> Result<SchemaEntry, String> {
    match tref {
        TableRef::Table { name, alias } => {
            if let Some(cte) = ctes.get(name) {
                return Ok(SchemaEntry {
                    alias: alias.clone(),
                    columns: cte.columns.clone(),
                });
            }
            let tbl = catalog
                .get_table(name)
                .ok_or_else(|| format!("Virtual catalog relation '{name}' not found"))?;
            Ok(SchemaEntry {
                alias: alias.clone(),
                columns: tbl.columns.clone(),
            })
        }
        TableRef::Subquery { query, alias } => {
            let cols = describe_query(catalog, query, ctes)?;
            Ok(SchemaEntry {
                alias: alias.clone(),
                columns: cols,
            })
        }
    }
}

fn infer_expr_pg_oid(expr: &Expr, schemas: &[SchemaEntry]) -> i32 {
    match expr {
        Expr::Literal(v) => v.pg_type_oid(),
        Expr::Ident { table, column } => {
            for s in schemas {
                if table.as_deref().is_none_or(|t| t == s.alias) {
                    if let Some(col) = s
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(column))
                    {
                        return col.pg_type_oid;
                    }
                }
            }
            25
        }
        Expr::Cast { target_type, .. } => match target_type.as_str() {
            "bool" | "boolean" => 16,
            "int2" | "smallint" => 21,
            "int" | "int4" | "integer" => 23,
            "int8" | "bigint" | "oid" | "regclass" | "regnamespace" | "regtype" => 20,
            "float4" | "real" => 700,
            "float8" | "double precision" => 701,
            "jsonb" => 3802,
            _ => 25,
        },
        Expr::Function { name, .. } => match name.as_str() {
            "count" | "pg_backend_pid" | "array_length" | "array_position" | "length"
            | "char_length" => 20,
            "pg_table_is_visible" => 16,
            "current_schemas" => 1009,
            "array_agg" => 1009,
            _ => 25,
        },
        Expr::Binary { op, .. } if is_bool_op(op) => 16,
        Expr::IsNull { .. }
        | Expr::IsBool { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::AnyOp { .. }
        | Expr::Between { .. }
        | Expr::Exists(_)
        | Expr::UnaryNot(_) => 16,
        Expr::Case {
            whens, else_expr, ..
        } => whens
            .first()
            .map(|(_, v)| infer_expr_pg_oid(v, schemas))
            .or_else(|| else_expr.as_ref().map(|e| infer_expr_pg_oid(e, schemas)))
            .unwrap_or(25),
        _ => 25,
    }
}

fn is_bool_op(op: &BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq
            | BinOp::Neq
            | BinOp::Lt
            | BinOp::Lte
            | BinOp::Gt
            | BinOp::Gte
            | BinOp::And
            | BinOp::Or
            | BinOp::Like
            | BinOp::ILike
            | BinOp::NotLike
            | BinOp::NotILike
            | BinOp::ContainsArr
            | BinOp::ContainedByArr
            | BinOp::Regex
            | BinOp::NotRegex
    )
}

fn eval_query(
    catalog: &VirtualCatalog,
    query: &CatalogQuery,
    outer_ctes: &BTreeMap<String, CatalogTable>,
    outer_row: Option<&BoundRow>,
) -> Result<CatalogTable, String> {
    let mut ctes = outer_ctes.clone();
    for (name, cte_q) in &query.ctes {
        let materialized = eval_query(catalog, cte_q, &ctes, outer_row)?;
        ctes.insert(name.clone(), materialized);
    }

    let mut combined_cols: Vec<CatalogColumn> = Vec::new();
    let mut combined_rows: Vec<Vec<CatalogValue>> = Vec::new();

    for (idx, (union_all, branch)) in query.branches.iter().enumerate() {
        let branch_tbl = eval_select_branch(catalog, branch, &ctes, outer_row)?;
        if idx == 0 {
            combined_cols = branch_tbl.columns;
            combined_rows = branch_tbl.rows;
        } else {
            if !union_all {
                let mut seen: HashSet<Vec<Option<String>>> = combined_rows
                    .iter()
                    .map(|r| r.iter().map(CatalogValue::to_pg_text).collect())
                    .collect();
                for row in branch_tbl.rows {
                    let key: Vec<Option<String>> =
                        row.iter().map(CatalogValue::to_pg_text).collect();
                    if seen.insert(key) {
                        combined_rows.push(row);
                    }
                }
            } else {
                combined_rows.extend(branch_tbl.rows);
            }
        }
    }

    if !query.order_by.is_empty() && !combined_rows.is_empty() {
        apply_order_by_projected(
            catalog,
            &ctes,
            &combined_cols,
            &mut combined_rows,
            &query.order_by,
        )?;
    }

    if let Some(off) = query.offset {
        if off >= combined_rows.len() {
            combined_rows.clear();
        } else {
            combined_rows.drain(0..off);
        }
    }

    if let Some(lim) = query.limit {
        combined_rows.truncate(lim);
    }

    Ok(CatalogTable {
        schema: String::new(),
        name: "result".into(),
        columns: combined_cols,
        rows: combined_rows,
    })
}

fn resolve_table_ref(
    catalog: &VirtualCatalog,
    tref: &TableRef,
    ctes: &BTreeMap<String, CatalogTable>,
    outer_row: Option<&BoundRow>,
) -> Result<(String, CatalogTable), String> {
    match tref {
        TableRef::Table { name, alias } => {
            if let Some(cte) = ctes.get(name) {
                return Ok((alias.clone(), cte.clone()));
            }
            let tbl = catalog
                .get_table(name)
                .ok_or_else(|| format!("Catalog relation '{name}' does not exist"))?;
            Ok((alias.clone(), tbl.clone()))
        }
        TableRef::Subquery { query, alias } => {
            let tbl = eval_query(catalog, query, ctes, outer_row)?;
            Ok((alias.clone(), tbl))
        }
    }
}

fn eval_select_branch(
    catalog: &VirtualCatalog,
    branch: &CatalogSelect,
    ctes: &BTreeMap<String, CatalogTable>,
    outer_row: Option<&BoundRow>,
) -> Result<CatalogTable, String> {
    let mut rows: Vec<BoundRow> = if let Some(ref from_ref) = branch.from {
        let (alias, tbl) = resolve_table_ref(catalog, from_ref, ctes, outer_row)?;
        tbl.rows
            .iter()
            .map(|r| BoundRow::from_table(alias.clone(), &tbl.columns, r))
            .collect()
    } else {
        vec![BoundRow::empty()]
    };

    for join in &branch.joins {
        let (right_alias, right_tbl) = resolve_table_ref(catalog, &join.target, ctes, outer_row)?;
        let mut next_rows = Vec::new();
        let right_bound: Vec<BoundRow> = right_tbl
            .rows
            .iter()
            .map(|r| BoundRow::from_table(right_alias.clone(), &right_tbl.columns, r))
            .collect();

        match join.kind {
            JoinKind::Cross => {
                for l in &rows {
                    for r in &right_bound {
                        next_rows.push(l.combine(r));
                    }
                }
            }
            JoinKind::Inner => {
                for l in &rows {
                    for r in &right_bound {
                        let combined = l.combine(r);
                        if eval_join_cond(catalog, ctes, &combined, join, outer_row)? {
                            next_rows.push(combined);
                        }
                    }
                }
            }
            JoinKind::Left => {
                for l in &rows {
                    let mut matched = false;
                    for r in &right_bound {
                        let combined = l.combine(r);
                        if eval_join_cond(catalog, ctes, &combined, join, outer_row)? {
                            next_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        next_rows.push(l.combine_nulls(right_alias.clone(), &right_tbl.columns));
                    }
                }
            }
            JoinKind::Right => {
                let left_template_cols: Vec<CatalogColumn> = rows
                    .first()
                    .map(|l| {
                        l.sources
                            .iter()
                            .flat_map(|(_, ps)| ps.iter().map(|(c, _)| c.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                for r in &right_bound {
                    let mut matched = false;
                    for l in &rows {
                        let combined = l.combine(r);
                        if eval_join_cond(catalog, ctes, &combined, join, outer_row)? {
                            next_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = BoundRow::empty();
                        combined = combined.combine_nulls("_left".into(), &left_template_cols);
                        next_rows.push(combined.combine(r));
                    }
                }
            }
        }
        rows = next_rows;
    }

    if let Some(ref w) = branch.where_clause {
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(merged) = merge_outer_row(outer_row, &row) {
                let v = eval_expr(catalog, ctes, w, &merged)?;
                if v.as_bool() == Some(true) {
                    filtered.push(row);
                }
            }
        }
        rows = filtered;
    }

    let schemas = collect_branch_schemas(catalog, branch, ctes)?;
    let has_agg = !branch.group_by.is_empty()
        || branch.items.iter().any(|it| match it {
            SelectItem::Expr { expr, .. } => expr_contains_agg(expr),
            _ => false,
        });

    let (out_cols, mut out_rows) = if has_agg {
        eval_grouped_projection(catalog, ctes, branch, &rows, &schemas, outer_row)?
    } else {
        eval_flat_projection(catalog, ctes, branch, &rows, &schemas, outer_row)?
    };

    if branch.distinct {
        let mut seen = HashSet::new();
        out_rows.retain(|row| {
            let key: Vec<Option<String>> = row.iter().map(CatalogValue::to_pg_text).collect();
            seen.insert(key)
        });
    }

    Ok(CatalogTable {
        schema: String::new(),
        name: "result".into(),
        columns: out_cols,
        rows: out_rows,
    })
}

fn merge_outer_row(outer_row: Option<&BoundRow>, row: &BoundRow) -> Option<BoundRow> {
    Some(match outer_row {
        Some(outer) => row.combine(outer),
        None => row.clone(),
    })
}

fn eval_join_cond(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    combined: &BoundRow,
    join: &crate::parser::JoinSpec,
    outer_row: Option<&BoundRow>,
) -> Result<bool, String> {
    if !join.using_cols.is_empty() {
        for ucol in &join.using_cols {
            let mut vals = Vec::new();
            for (_, pairs) in &combined.sources {
                for (c, v) in pairs {
                    if c.name.eq_ignore_ascii_case(ucol) {
                        vals.push(v);
                    }
                }
            }
            if vals.len() < 2
                || vals[0]
                    .cmp_sql(vals[vals.len() - 1])
                    .is_none_or(|o| o != std::cmp::Ordering::Equal)
            {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(ref on_expr) = join.on {
        let scope = merge_outer_row(outer_row, combined).unwrap_or_else(BoundRow::empty);
        let res = eval_expr(catalog, ctes, on_expr, &scope)?;
        Ok(res.as_bool() == Some(true))
    } else {
        Ok(true)
    }
}

fn eval_flat_projection(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    branch: &CatalogSelect,
    rows: &[BoundRow],
    schemas: &[SchemaEntry],
    outer_row: Option<&BoundRow>,
) -> Result<(Vec<CatalogColumn>, Vec<Vec<CatalogValue>>), String> {
    let cols = describe_items(&branch.items, schemas, rows.first());
    let mut out_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let scope = merge_outer_row(outer_row, row).unwrap_or_else(BoundRow::empty);
        let mut out_vals = Vec::with_capacity(cols.len());
        for item in &branch.items {
            match item {
                SelectItem::Wildcard => {
                    for (_, pairs) in &row.sources {
                        for (_, v) in pairs {
                            out_vals.push(v.clone());
                        }
                    }
                }
                SelectItem::QualifiedWildcard(alias) => {
                    if let Some((_, pairs)) = row.sources.iter().find(|(a, _)| a == alias) {
                        for (_, v) in pairs {
                            out_vals.push(v.clone());
                        }
                    }
                }
                SelectItem::Expr { expr, .. } => {
                    out_vals.push(eval_expr(catalog, ctes, expr, &scope)?);
                }
            }
        }
        out_rows.push(out_vals);
    }

    // Refine inferred column OIDs from actual first non-null row values if needed
    let mut refined_cols = cols;
    if let Some(first_row) = out_rows.first() {
        for (col, val) in refined_cols.iter_mut().zip(first_row.iter()) {
            if !val.is_null() && col.pg_type_oid == 25 {
                col.pg_type_oid = val.pg_type_oid();
            }
        }
    }

    Ok((refined_cols, out_rows))
}

fn eval_grouped_projection(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    branch: &CatalogSelect,
    rows: &[BoundRow],
    schemas: &[SchemaEntry],
    outer_row: Option<&BoundRow>,
) -> Result<(Vec<CatalogColumn>, Vec<Vec<CatalogValue>>), String> {
    let cols = describe_items(&branch.items, schemas, rows.first());
    let mut groups: BTreeMap<Vec<Option<String>>, Vec<&BoundRow>> = BTreeMap::new();

    if branch.group_by.is_empty() {
        groups.insert(Vec::new(), rows.iter().collect());
    } else {
        for row in rows {
            let scope = merge_outer_row(outer_row, row).unwrap_or_else(BoundRow::empty);
            let mut key = Vec::with_capacity(branch.group_by.len());
            for g in &branch.group_by {
                key.push(eval_expr(catalog, ctes, g, &scope)?.to_pg_text());
            }
            groups.entry(key).or_default().push(row);
        }
    }

    let mut out_rows = Vec::with_capacity(groups.len());
    for (_, group_rows) in groups {
        let mut vals = Vec::with_capacity(branch.items.len());
        for item in &branch.items {
            if let SelectItem::Expr { expr, .. } = item {
                vals.push(eval_agg_expr(catalog, ctes, expr, &group_rows, outer_row)?);
            }
        }
        out_rows.push(vals);
    }

    Ok((cols, out_rows))
}

fn eval_agg_expr(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    expr: &Expr,
    group: &[&BoundRow],
    outer_row: Option<&BoundRow>,
) -> Result<CatalogValue, String> {
    if let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    {
        match name.as_str() {
            "count" => {
                if args.is_empty() {
                    return Ok(CatalogValue::Int(group.len() as i64));
                }
                let mut cnt = 0i64;
                let mut seen = HashSet::new();
                for r in group {
                    let scope = merge_outer_row(outer_row, r).unwrap_or_else(BoundRow::empty);
                    let v = eval_expr(catalog, ctes, &args[0], &scope)?;
                    if !v.is_null() && (!distinct || seen.insert(v.to_pg_text())) {
                        cnt += 1;
                    }
                }
                return Ok(CatalogValue::Int(cnt));
            }
            "array_agg" => {
                let mut items = Vec::new();
                let mut seen = HashSet::new();
                for r in group {
                    let scope = merge_outer_row(outer_row, r).unwrap_or_else(BoundRow::empty);
                    if let Some(arg) = args.first() {
                        let v = eval_expr(catalog, ctes, arg, &scope)?;
                        if !v.is_null() && (!distinct || seen.insert(v.to_pg_text())) {
                            items.push(v);
                        }
                    }
                }
                if items
                    .iter()
                    .all(|v| matches!(v, CatalogValue::Int(_) | CatalogValue::Bool(_)))
                {
                    let ints = items.iter().filter_map(CatalogValue::as_i64).collect();
                    return Ok(CatalogValue::IntArray(ints));
                }
                let texts = items.iter().map(CatalogValue::to_string_lossy).collect();
                return Ok(CatalogValue::TextArray(texts));
            }
            "string_agg" => {
                let sep = if let Some(s_expr) = args.get(1) {
                    let scope = group
                        .first()
                        .and_then(|r| merge_outer_row(outer_row, r))
                        .unwrap_or_else(BoundRow::empty);
                    eval_expr(catalog, ctes, s_expr, &scope)?.to_string_lossy()
                } else {
                    ",".to_string()
                };
                let mut parts = Vec::new();
                for r in group {
                    let scope = merge_outer_row(outer_row, r).unwrap_or_else(BoundRow::empty);
                    if let Some(arg) = args.first() {
                        let v = eval_expr(catalog, ctes, arg, &scope)?;
                        if !v.is_null() {
                            parts.push(v.to_string_lossy());
                        }
                    }
                }
                return Ok(CatalogValue::Text(parts.join(&sep)));
            }
            "max" | "min" => {
                let mut best: Option<CatalogValue> = None;
                for r in group {
                    let scope = merge_outer_row(outer_row, r).unwrap_or_else(BoundRow::empty);
                    if let Some(arg) = args.first() {
                        let v = eval_expr(catalog, ctes, arg, &scope)?;
                        if v.is_null() {
                            continue;
                        }
                        best = Some(match best {
                            None => v,
                            Some(cur) => match v.cmp_sql(&cur) {
                                Some(std::cmp::Ordering::Greater) if name == "max" => v,
                                Some(std::cmp::Ordering::Less) if name == "min" => v,
                                _ => cur,
                            },
                        });
                    }
                }
                return Ok(best.unwrap_or(CatalogValue::Null));
            }
            _ => {}
        }
    }

    if let Some(first) = group.first() {
        let scope = merge_outer_row(outer_row, first).unwrap_or_else(BoundRow::empty);
        eval_expr(catalog, ctes, expr, &scope)
    } else {
        Ok(CatalogValue::Null)
    }
}

fn expr_contains_agg(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            matches!(
                name.as_str(),
                "count" | "array_agg" | "string_agg" | "max" | "min" | "json_agg"
            ) || args.iter().any(expr_contains_agg)
        }
        Expr::Binary { left, right, .. } => expr_contains_agg(left) || expr_contains_agg(right),
        Expr::Cast { expr, .. } => expr_contains_agg(expr),
        _ => false,
    }
}

fn describe_items(
    items: &[SelectItem],
    schemas: &[SchemaEntry],
    _sample_row: Option<&BoundRow>,
) -> Vec<CatalogColumn> {
    let mut cols = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for s in schemas {
                    cols.extend(s.columns.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(alias) => {
                if let Some(s) = schemas.iter().find(|s| s.alias == *alias) {
                    cols.extend(s.columns.iter().cloned());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| default_col_name(expr));
                let oid = infer_expr_pg_oid(expr, schemas);
                cols.push(CatalogColumn::new(name, oid));
            }
        }
    }
    cols
}

fn default_col_name(expr: &Expr) -> String {
    match expr {
        Expr::Ident { column, .. } => column.clone(),
        Expr::SysVar(sv) => format!("@@{sv}"),
        Expr::Function { name, .. } => {
            if name.eq_ignore_ascii_case("user") {
                "current_user".to_string()
            } else {
                name.clone()
            }
        }
        Expr::Cast { expr, .. } => default_col_name(expr),
        _ => "?column?".to_string(),
    }
}

fn apply_order_by_projected(
    _catalog: &VirtualCatalog,
    _ctes: &BTreeMap<String, CatalogTable>,
    cols: &[CatalogColumn],
    rows: &mut [Vec<CatalogValue>],
    order_by: &[crate::parser::OrderItem],
) -> Result<(), String> {
    let indices: Vec<(usize, bool)> = order_by
        .iter()
        .filter_map(|item| {
            let idx = match &item.expr {
                Expr::Literal(CatalogValue::Int(pos)) if *pos >= 1 => (*pos as usize) - 1,
                Expr::Ident { column, .. } => cols
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column))?,
                Expr::Function { name, .. } => cols
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))?,
                _ => return None,
            };
            (idx < cols.len()).then_some((idx, item.asc))
        })
        .collect();

    if indices.is_empty() {
        return Ok(());
    }

    rows.sort_by(|a, b| {
        for &(idx, asc) in &indices {
            let va = &a[idx];
            let vb = &b[idx];
            let ord = match (va.is_null(), vb.is_null()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => va.cmp_sql(vb).unwrap_or(std::cmp::Ordering::Equal),
            };
            if ord != std::cmp::Ordering::Equal {
                return if asc { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(())
}

fn eval_expr(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    expr: &Expr,
    row: &BoundRow,
) -> Result<CatalogValue, String> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(_) => Ok(CatalogValue::Null),
        Expr::SysVar(sv) => Ok(eval_mysql_sysvar(catalog, sv)),
        Expr::Ident { table, column } => {
            if table.is_none() {
                match column.as_str() {
                    "current_user" | "session_user" | "user" => {
                        return Ok(CatalogValue::Text(match catalog.dialect {
                            CatalogDialect::Postgres => "postgres".into(),
                            CatalogDialect::MySql { .. } => "root@localhost".into(),
                        }));
                    }
                    "current_schema" => {
                        return Ok(CatalogValue::Text(match &catalog.dialect {
                            CatalogDialect::Postgres => "public".into(),
                            CatalogDialect::MySql { database } => database.clone(),
                        }));
                    }
                    "current_database" => {
                        return Ok(CatalogValue::Text(catalog.database_name.clone()));
                    }
                    _ => {}
                }
            }
            Ok(row
                .lookup(table.as_deref(), column)
                .cloned()
                .unwrap_or(CatalogValue::Null))
        }
        Expr::UnaryNot(inner) => {
            let v = eval_expr(catalog, ctes, inner, row)?;
            Ok(match v.as_bool() {
                Some(b) => CatalogValue::Bool(!b),
                None => CatalogValue::Null,
            })
        }
        Expr::UnaryNeg(inner) => {
            let v = eval_expr(catalog, ctes, inner, row)?;
            Ok(match v {
                CatalogValue::Int(i) => CatalogValue::Int(-i),
                CatalogValue::Float(f) => CatalogValue::Float(-f),
                _ => CatalogValue::Null,
            })
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_expr(catalog, ctes, expr, row)?;
            let is_n = v.is_null();
            Ok(CatalogValue::Bool(if *negated { !is_n } else { is_n }))
        }
        Expr::IsBool {
            expr,
            expected,
            negated,
        } => {
            let v = eval_expr(catalog, ctes, expr, row)?;
            let matched = v.as_bool() == Some(*expected);
            Ok(CatalogValue::Bool(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Binary { left, op, right } => eval_binary(catalog, ctes, left, op, right, row),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(catalog, ctes, expr, row)?;
            let lo = eval_expr(catalog, ctes, low, row)?;
            let hi = eval_expr(catalog, ctes, high, row)?;
            let in_range = matches!(
                (val.cmp_sql(&lo), val.cmp_sql(&hi)),
                (
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            );
            Ok(CatalogValue::Bool(if *negated {
                !in_range
            } else {
                in_range
            }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr(catalog, ctes, expr, row)?;
            if val.is_null() {
                return Ok(CatalogValue::Null);
            }
            let mut found = false;
            for item in list {
                let iv = eval_expr(catalog, ctes, item, row)?;
                if val.cmp_sql(&iv) == Some(std::cmp::Ordering::Equal) {
                    found = true;
                    break;
                }
            }
            Ok(CatalogValue::Bool(if *negated { !found } else { found }))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let val = eval_expr(catalog, ctes, expr, row)?;
            let sub = eval_query(catalog, subquery, ctes, Some(row))?;
            let found = sub.rows.iter().any(|r| {
                r.first()
                    .and_then(|rv| val.cmp_sql(rv))
                    .is_some_and(|o| o == std::cmp::Ordering::Equal)
            });
            Ok(CatalogValue::Bool(if *negated { !found } else { found }))
        }
        Expr::AnyOp { left, op, array } => {
            let lval = eval_expr(catalog, ctes, left, row)?;
            let rval = eval_expr(catalog, ctes, array, row)?;
            match op {
                BinOp::Eq => Ok(CatalogValue::Bool(rval.contains_element(&lval))),
                BinOp::Neq => Ok(CatalogValue::Bool(!rval.contains_element(&lval))),
                _ => Ok(CatalogValue::Bool(false)),
            }
        }
        Expr::ArrayLit(elems) => {
            let mut vals = Vec::with_capacity(elems.len());
            for e in elems {
                vals.push(eval_expr(catalog, ctes, e, row)?);
            }
            if !vals.is_empty() && vals.iter().all(|v| matches!(v, CatalogValue::Int(_))) {
                Ok(CatalogValue::IntArray(
                    vals.into_iter().filter_map(|v| v.as_i64()).collect(),
                ))
            } else {
                Ok(CatalogValue::TextArray(
                    vals.into_iter().map(|v| v.to_string_lossy()).collect(),
                ))
            }
        }
        Expr::Cast { expr, target_type } => {
            let val = eval_expr(catalog, ctes, expr, row)?;
            Ok(eval_cast(catalog, val, target_type))
        }
        Expr::Function { name, args, .. } => eval_scalar_fn(catalog, ctes, name, args, row),
        Expr::Case {
            operand,
            whens,
            else_expr,
        } => {
            if let Some(op_expr) = operand {
                let target = eval_expr(catalog, ctes, op_expr, row)?;
                for (cond, val_expr) in whens {
                    let cand = eval_expr(catalog, ctes, cond, row)?;
                    if target.cmp_sql(&cand) == Some(std::cmp::Ordering::Equal) {
                        return eval_expr(catalog, ctes, val_expr, row);
                    }
                }
            } else {
                for (cond, val_expr) in whens {
                    let cond_val = eval_expr(catalog, ctes, cond, row)?;
                    if cond_val.as_bool() == Some(true) {
                        return eval_expr(catalog, ctes, val_expr, row);
                    }
                }
            }
            if let Some(el) = else_expr {
                eval_expr(catalog, ctes, el, row)
            } else {
                Ok(CatalogValue::Null)
            }
        }
        Expr::Subquery(sub) => {
            let res = eval_query(catalog, sub, ctes, Some(row))?;
            Ok(res
                .rows
                .first()
                .and_then(|r| r.first())
                .cloned()
                .unwrap_or(CatalogValue::Null))
        }
        Expr::Exists(sub) => {
            let res = eval_query(catalog, sub, ctes, Some(row))?;
            Ok(CatalogValue::Bool(!res.rows.is_empty()))
        }
    }
}

fn eval_binary(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    left: &Expr,
    op: &BinOp,
    right: &Expr,
    row: &BoundRow,
) -> Result<CatalogValue, String> {
    match op {
        BinOp::And => {
            let lv = eval_expr(catalog, ctes, left, row)?;
            if lv.as_bool() == Some(false) {
                return Ok(CatalogValue::Bool(false));
            }
            let rv = eval_expr(catalog, ctes, right, row)?;
            return Ok(match (lv.as_bool(), rv.as_bool()) {
                (Some(true), Some(true)) => CatalogValue::Bool(true),
                (_, Some(false)) => CatalogValue::Bool(false),
                _ => CatalogValue::Null,
            });
        }
        BinOp::Or => {
            let lv = eval_expr(catalog, ctes, left, row)?;
            if lv.as_bool() == Some(true) {
                return Ok(CatalogValue::Bool(true));
            }
            let rv = eval_expr(catalog, ctes, right, row)?;
            return Ok(match (lv.as_bool(), rv.as_bool()) {
                (Some(true), _) | (_, Some(true)) => CatalogValue::Bool(true),
                (Some(false), Some(false)) => CatalogValue::Bool(false),
                _ => CatalogValue::Null,
            });
        }
        _ => {}
    }

    let lv = eval_expr(catalog, ctes, left, row)?;
    let rv = eval_expr(catalog, ctes, right, row)?;

    Ok(match op {
        BinOp::Concat => {
            if lv.is_null() || rv.is_null() {
                CatalogValue::Null
            } else {
                CatalogValue::Text(format!("{}{}", lv.to_string_lossy(), rv.to_string_lossy()))
            }
        }
        BinOp::Add => num_arith(&lv, &rv, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => num_arith(&lv, &rv, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => num_arith(&lv, &rv, |a, b| a * b, |a, b| a * b),
        BinOp::Div => num_arith(
            &lv,
            &rv,
            |a, b| if b == 0 { 0 } else { a / b },
            |a, b| a / b,
        ),
        BinOp::Eq => CatalogValue::Bool(lv.cmp_sql(&rv) == Some(std::cmp::Ordering::Equal)),
        BinOp::Neq => CatalogValue::Bool(
            !lv.is_null() && !rv.is_null() && lv.cmp_sql(&rv) != Some(std::cmp::Ordering::Equal),
        ),
        BinOp::Lt => CatalogValue::Bool(lv.cmp_sql(&rv) == Some(std::cmp::Ordering::Less)),
        BinOp::Lte => CatalogValue::Bool(matches!(
            lv.cmp_sql(&rv),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        )),
        BinOp::Gt => CatalogValue::Bool(lv.cmp_sql(&rv) == Some(std::cmp::Ordering::Greater)),
        BinOp::Gte => CatalogValue::Bool(matches!(
            lv.cmp_sql(&rv),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        )),
        BinOp::Like => CatalogValue::Bool(sql_like(
            &lv.to_string_lossy(),
            &rv.to_string_lossy(),
            false,
        )),
        BinOp::ILike => {
            CatalogValue::Bool(sql_like(&lv.to_string_lossy(), &rv.to_string_lossy(), true))
        }
        BinOp::NotLike => CatalogValue::Bool(!sql_like(
            &lv.to_string_lossy(),
            &rv.to_string_lossy(),
            false,
        )),
        BinOp::NotILike => CatalogValue::Bool(!sql_like(
            &lv.to_string_lossy(),
            &rv.to_string_lossy(),
            true,
        )),
        BinOp::ContainsArr => CatalogValue::Bool(lv.contains_all(&rv)),
        BinOp::ContainedByArr => CatalogValue::Bool(rv.contains_all(&lv)),
        BinOp::Regex => {
            let s = lv.to_string_lossy().to_ascii_lowercase();
            let p = rv
                .to_string_lossy()
                .replace(['^', '$'], "")
                .to_ascii_lowercase();
            CatalogValue::Bool(s.contains(&p))
        }
        BinOp::NotRegex => {
            let s = lv.to_string_lossy().to_ascii_lowercase();
            let p = rv
                .to_string_lossy()
                .replace(['^', '$'], "")
                .to_ascii_lowercase();
            CatalogValue::Bool(!s.contains(&p))
        }
        BinOp::And | BinOp::Or => unreachable!(),
    })
}

fn num_arith(
    l: &CatalogValue,
    r: &CatalogValue,
    int_op: fn(i64, i64) -> i64,
    flt_op: fn(f64, f64) -> f64,
) -> CatalogValue {
    match (l, r) {
        (CatalogValue::Int(a), CatalogValue::Int(b)) => CatalogValue::Int(int_op(*a, *b)),
        _ => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => CatalogValue::Float(flt_op(a, b)),
            _ => CatalogValue::Null,
        },
    }
}

fn eval_cast(catalog: &VirtualCatalog, val: CatalogValue, target_type: &str) -> CatalogValue {
    if val.is_null() {
        return CatalogValue::Null;
    }
    match target_type {
        "regclass" => {
            if let Some(oid) = val.as_i64() {
                CatalogValue::Int(oid)
            } else {
                let key = normalize_ident(&val.to_string_lossy());
                let oid = catalog.relation_oids.get(&key).copied().unwrap_or(0);
                CatalogValue::Int(oid)
            }
        }
        "regnamespace" => match normalize_ident(&val.to_string_lossy()).as_str() {
            "public" => CatalogValue::Int(2200),
            "pg_catalog" => CatalogValue::Int(11),
            "information_schema" => CatalogValue::Int(12345),
            _ => CatalogValue::Int(0),
        },
        "oid" | "int" | "int2" | "int4" | "int8" | "integer" | "smallint" | "bigint" => {
            val.as_i64().map_or(CatalogValue::Null, CatalogValue::Int)
        }
        "float4" | "float8" | "real" | "double precision" | "numeric" => {
            val.as_f64().map_or(CatalogValue::Null, CatalogValue::Float)
        }
        "bool" | "boolean" => val.as_bool().map_or(CatalogValue::Null, CatalogValue::Bool),
        "text" | "varchar" | "character varying" | "name" => {
            CatalogValue::Text(val.to_string_lossy())
        }
        "text[]" => match val {
            CatalogValue::TextArray(_) => val,
            CatalogValue::Text(s) => {
                let trimmed = s.trim().trim_start_matches('{').trim_end_matches('}');
                let items = trimmed
                    .split(',')
                    .map(|p| p.trim().trim_matches('"').to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                CatalogValue::TextArray(items)
            }
            other => CatalogValue::TextArray(vec![other.to_string_lossy()]),
        },
        _ => val,
    }
}

fn eval_scalar_fn(
    catalog: &VirtualCatalog,
    ctes: &BTreeMap<String, CatalogTable>,
    name: &str,
    args: &[Expr],
    row: &BoundRow,
) -> Result<CatalogValue, String> {
    let mut vals = Vec::with_capacity(args.len());
    for a in args {
        vals.push(eval_expr(catalog, ctes, a, row)?);
    }

    Ok(match name {
        "version" => CatalogValue::Text(match catalog.dialect {
            CatalogDialect::Postgres => format!(
                "PostgreSQL 16.0 (TapisaurusDB Universal Safe-Rust Engine v{})",
                catalog.server_version
            ),
            CatalogDialect::MySql { .. } => "8.0.35-TapisaurusDB".into(),
        }),
        "current_setting" => {
            let key = vals
                .first()
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let val = match key.as_str() {
                "server_version_num" => "160000",
                "server_version" => "16.0",
                "client_encoding" | "server_encoding" => "UTF8",
                "standard_conforming_strings" => "on",
                "timezone" => "UTC",
                "transaction_isolation" => "read committed",
                "search_path" => "\"$user\", public",
                _ => "on",
            };
            CatalogValue::Text(val.into())
        }
        "current_database" | "database" => CatalogValue::Text(catalog.database_name.clone()),
        "current_schema" | "schema" => CatalogValue::Text(match &catalog.dialect {
            CatalogDialect::Postgres => "public".into(),
            CatalogDialect::MySql { database } => database.clone(),
        }),
        "current_schemas" => {
            let include_implicit = vals
                .first()
                .and_then(CatalogValue::as_bool)
                .unwrap_or(false);
            if include_implicit {
                CatalogValue::TextArray(vec!["pg_catalog".into(), "public".into()])
            } else {
                CatalogValue::TextArray(vec!["public".into()])
            }
        }
        "current_user" | "session_user" | "user" => CatalogValue::Text(match catalog.dialect {
            CatalogDialect::Postgres => "postgres".into(),
            CatalogDialect::MySql { .. } => "root@localhost".into(),
        }),
        "pg_backend_pid" => CatalogValue::Int(1001),
        "txid_current" | "pg_current_xact_id" => CatalogValue::Int(1),
        "pg_is_in_recovery" => CatalogValue::Bool(false),
        "pg_current_wal_lsn" => CatalogValue::Text("0/1000000".into()),
        "set_config" => vals
            .get(1)
            .cloned()
            .unwrap_or_else(|| CatalogValue::Text(String::new())),
        "pg_table_is_visible" => CatalogValue::Bool(true),
        "pg_encoding_to_char" => CatalogValue::Text("UTF8".into()),
        "pg_get_userbyid" => CatalogValue::Text("postgres".into()),
        "format_type" => {
            let oid = vals.first().and_then(CatalogValue::as_i64).unwrap_or(25);
            let typmod = vals.get(1).and_then(CatalogValue::as_i64).unwrap_or(-1);
            CatalogValue::Text(format_pg_type(oid, typmod))
        }
        "pg_get_expr" => vals.first().cloned().unwrap_or(CatalogValue::Null),
        "obj_description" => {
            let oid = vals.first().and_then(CatalogValue::as_i64).unwrap_or(0);
            lookup_description(catalog, oid, 0)
        }
        "col_description" => {
            let oid = vals.first().and_then(CatalogValue::as_i64).unwrap_or(0);
            let sub = vals.get(1).and_then(CatalogValue::as_i64).unwrap_or(0);
            lookup_description(catalog, oid, sub)
        }
        "pg_get_indexdef" => {
            let idx_oid = vals.first().and_then(CatalogValue::as_i64).unwrap_or(0);
            lookup_indexdef(catalog, idx_oid)
        }
        "pg_get_constraintdef" => {
            let con_oid = vals.first().and_then(CatalogValue::as_i64).unwrap_or(0);
            lookup_constraintdef(catalog, con_oid)
        }
        "coalesce" => vals
            .into_iter()
            .find(|v| !v.is_null())
            .unwrap_or(CatalogValue::Null),
        "nullif" => {
            if let (Some(a), Some(b)) = (vals.first(), vals.get(1)) {
                if a.cmp_sql(b) == Some(std::cmp::Ordering::Equal) {
                    CatalogValue::Null
                } else {
                    a.clone()
                }
            } else {
                CatalogValue::Null
            }
        }
        "lower" => vals
            .first()
            .filter(|v| !v.is_null())
            .map_or(CatalogValue::Null, |v| {
                CatalogValue::Text(v.to_string_lossy().to_ascii_lowercase())
            }),
        "upper" => vals
            .first()
            .filter(|v| !v.is_null())
            .map_or(CatalogValue::Null, |v| {
                CatalogValue::Text(v.to_string_lossy().to_ascii_uppercase())
            }),
        "trim" => vals
            .first()
            .filter(|v| !v.is_null())
            .map_or(CatalogValue::Null, |v| {
                CatalogValue::Text(v.to_string_lossy().trim().to_string())
            }),
        "quote_ident" => vals
            .first()
            .filter(|v| !v.is_null())
            .map_or(CatalogValue::Null, |v| {
                CatalogValue::Text(v.to_string_lossy())
            }),
        "concat" => {
            let s: String = vals
                .into_iter()
                .filter(|v| !v.is_null())
                .map(|v| v.to_string_lossy())
                .collect();
            CatalogValue::Text(s)
        }
        "concat_ws" => {
            let sep = vals
                .first()
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default();
            let parts: Vec<String> = vals
                .into_iter()
                .skip(1)
                .filter(|v| !v.is_null())
                .map(|v| v.to_string_lossy())
                .collect();
            CatalogValue::Text(parts.join(&sep))
        }
        "replace" => {
            let s = vals
                .first()
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default();
            let from = vals
                .get(1)
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default();
            let to = vals
                .get(2)
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default();
            CatalogValue::Text(s.replace(&from, &to))
        }
        "array_to_string" => {
            let sep = vals
                .get(1)
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_else(|| ",".into());
            match vals.first() {
                Some(CatalogValue::IntArray(arr)) => {
                    let s: Vec<String> = arr.iter().map(ToString::to_string).collect();
                    CatalogValue::Text(s.join(&sep))
                }
                Some(CatalogValue::TextArray(arr)) => CatalogValue::Text(arr.join(&sep)),
                Some(other) if !other.is_null() => CatalogValue::Text(other.to_string_lossy()),
                _ => CatalogValue::Null,
            }
        }
        "array_length" => match vals.first() {
            Some(CatalogValue::IntArray(arr)) => CatalogValue::Int(arr.len() as i64),
            Some(CatalogValue::TextArray(arr)) => CatalogValue::Int(arr.len() as i64),
            _ => CatalogValue::Null,
        },
        "array_position" => {
            if let (Some(arr), Some(needle)) = (vals.first(), vals.get(1)) {
                match arr {
                    CatalogValue::IntArray(a) => {
                        if let Some(n) = needle.as_i64() {
                            a.iter()
                                .position(|&x| x == n)
                                .map_or(CatalogValue::Null, |p| CatalogValue::Int((p + 1) as i64))
                        } else {
                            CatalogValue::Null
                        }
                    }
                    CatalogValue::TextArray(a) => {
                        let s = needle.to_string_lossy();
                        a.iter()
                            .position(|x| x == &s)
                            .map_or(CatalogValue::Null, |p| CatalogValue::Int((p + 1) as i64))
                    }
                    _ => CatalogValue::Null,
                }
            } else {
                CatalogValue::Null
            }
        }
        "string_to_array" => {
            let s = vals
                .first()
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_default();
            let sep = vals
                .get(1)
                .map(CatalogValue::to_string_lossy)
                .unwrap_or_else(|| ",".into());
            CatalogValue::TextArray(s.split(&sep).map(str::to_string).collect())
        }
        "length" | "char_length" => vals
            .first()
            .filter(|v| !v.is_null())
            .map_or(CatalogValue::Null, |v| {
                CatalogValue::Int(v.to_string_lossy().chars().count() as i64)
            }),
        "now" | "current_timestamp" => CatalogValue::Text("2026-01-01T00:00:00Z".into()),
        _ => vals.into_iter().next().unwrap_or(CatalogValue::Null),
    })
}

fn eval_mysql_sysvar(catalog: &VirtualCatalog, var: &str) -> CatalogValue {
    let clean = var.rsplit('.').next().unwrap_or(var);
    let val = match clean {
        "version_comment" => "TapisaurusDB Universal Multi-Model Engine (Safe Rust)",
        "version" => "8.0.35-TapisaurusDB",
        "collation_connection" | "collation_server" => "utf8mb4_general_ci",
        "character_set_client" | "character_set_connection" | "character_set_results" => "utf8mb4",
        "max_allowed_packet" => "67108864",
        "sql_mode" => "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
        "time_zone" => "SYSTEM",
        "system_time_zone" => "UTC",
        "tx_isolation" | "transaction_isolation" => "READ-COMMITTED",
        "tx_read_only" | "transaction_read_only" => "0",
        "auto_increment_increment" => "1",
        "lower_case_table_names" => "0",
        _ => catalog.server_version.as_str(),
    };
    CatalogValue::Text(val.into())
}

fn lookup_description(catalog: &VirtualCatalog, oid: i64, sub: i64) -> CatalogValue {
    let Some(tbl) = catalog.get_table("pg_catalog.pg_description") else {
        return CatalogValue::Null;
    };
    for r in &tbl.rows {
        if r.first().and_then(CatalogValue::as_i64) == Some(oid)
            && r.get(2).and_then(CatalogValue::as_i64) == Some(sub)
        {
            return r.get(3).cloned().unwrap_or(CatalogValue::Null);
        }
    }
    CatalogValue::Null
}

fn lookup_indexdef(catalog: &VirtualCatalog, idx_oid: i64) -> CatalogValue {
    let Some(class_tbl) = catalog.get_table("pg_catalog.pg_class") else {
        return CatalogValue::Null;
    };
    let idx_name = class_tbl.rows.iter().find_map(|r| {
        (r.first().and_then(CatalogValue::as_i64) == Some(idx_oid))
            .then(|| r.get(1).map(CatalogValue::to_string_lossy))
            .flatten()
    });
    if let (Some(name), Some(pg_idx)) = (idx_name, catalog.get_table("pg_catalog.pg_indexes")) {
        for r in &pg_idx.rows {
            if r.get(2).map(CatalogValue::to_string_lossy).as_deref() == Some(name.as_str()) {
                return r.get(4).cloned().unwrap_or(CatalogValue::Null);
            }
        }
    }
    CatalogValue::Null
}

fn lookup_constraintdef(catalog: &VirtualCatalog, con_oid: i64) -> CatalogValue {
    let Some(tbl) = catalog.get_table("pg_catalog.pg_constraint") else {
        return CatalogValue::Null;
    };
    for r in &tbl.rows {
        if r.first().and_then(CatalogValue::as_i64) == Some(con_oid) {
            return r.last().cloned().unwrap_or(CatalogValue::Null);
        }
    }
    CatalogValue::Null
}

fn sql_like(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    let s: Vec<char> = if case_insensitive {
        value.to_ascii_lowercase().chars().collect()
    } else {
        value.chars().collect()
    };
    let p: Vec<char> = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };
    let (sn, pn) = (s.len(), p.len());
    let mut dp = vec![vec![false; pn + 1]; sn + 1];
    dp[0][0] = true;
    for j in 1..=pn {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        } else {
            break;
        }
    }
    for i in 1..=sn {
        for j in 1..=pn {
            match p[j - 1] {
                '%' => dp[i][j] = dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i][j] = dp[i - 1][j - 1],
                c => dp[i][j] = dp[i - 1][j - 1] && s[i - 1] == c,
            }
        }
    }
    dp[sn][pn]
}
