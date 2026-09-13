"""Compare unchanged production AST after removing only reviewed observer statements."""
import ast


HOOKS = {
    'ec_bridge.start(globals())',
    "ec_bridge.capture(globals(), symbol, 'candidate_evaluation', m, score)",
    "ec_bridge.capture(globals(), symbol, 'trend_evaluation', m, score, context=ctx, trend_score=tscore)",
    "ec_bridge.capture(globals(), symbol, 'continuity_evaluation', m, score)",
    "ec_bridge.capture(globals(), symbol, 'early_notify_evaluation', m, score)",
    "ec_bridge.capture(globals(), symbol, 'early_watch_evaluation', m, score)",
    'ec_bridge.capture(globals(), symbol, event, m, score, source_id=ec_cursor.lastrowid, terminal=event, note=note)',
    'ec_bridge.research(globals(), event_id, event_type, symbol, m, score, shadow_score)',
    "ec_bridge.capture(globals(), symbol, 'episode_reset', m or {}, score, terminal=reason)",
    "ec_bridge.tick(sym, price, d.get('T'), recv_ms)",
}


class ObserverStatements(ast.NodeTransformer):
    def visit_Expr(self, node):
        if ast.unparse(node) in HOOKS: return None
        return self.generic_visit(node)

    def visit_Import(self, node):
        if ast.unparse(node) == 'import early_continuation_bridge as ec_bridge': return None
        return node

    def visit_Assign(self, node):
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == 'ec_cursor':
            # Original conn.execute is retained verbatim; only its discarded cursor is named.
            assert isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'conn.execute'
            return ast.Expr(value=node.value)
        return self.generic_visit(node)


def production_tree(tree):
    return ObserverStatements().visit(tree)
