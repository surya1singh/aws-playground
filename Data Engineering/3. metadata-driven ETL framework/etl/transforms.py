from pyspark.sql import functions as F

RULES = {
    "str.lower_trim": lambda args: __import__("etl.rules.string_rules", fromlist=["*"]).lower_trim(*args),
    "str.upper_trim": lambda args: __import__("etl.rules.string_rules", fromlist=["*"]).upper_trim(*args),
}

def build_select(df, columns_spec):
    exprs = []
    for c in columns_spec:
        name = c["name"]
        if "expr" in c:
            exprs.append(F.expr(c["expr"]).alias(name))
        elif "rule" in c:
            rule_fn = RULES[c["rule"]]
            # args are source column names
            exprs.append(rule_fn(c["args"]).alias(name))
        else:
            raise ValueError(f"Column {name} missing expr/rule")
    return df.select(*exprs)
