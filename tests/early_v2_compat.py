"""Recognize exact reviewed Early hooks; preserve all legacy AST fingerprints."""
import ast

SNIPPETS = [
'''if early_v2:
    early_v2.arm(st.active_radar_id, symbol, m, score)''',
'''if early_v2:
    await early_v2.premium(session, signal_id, symbol, m, plan)
else:
    await autotrade_handle_premium(session, signal_id, symbol, m, plan)''',
'''if early_v2:
    early_v2.tick(sym, price, ts, recv_ms, d.get("a"))''',
'''if early_v2 and await early_v2.callback(session, upd["callback_query"]):
    continue''',
'''if early_v2 and raw_text and await early_v2.command(session, raw_text, chat_id, user_id):
    continue''',
'''if early_v2:
    await telegram_send(session, "Premium AutoTrader: " + autotrade_cfg["mode"] + "\\n" + early_v2.status(), chat_id=chat_id)''',
'global early_v2',
'early_v2 = early_v2_adapter.Integration(globals())',
'early_v2 = None',
'import early_v2_adapter',
' tasks.append(early_v2.run(session))'.strip(),
]
APPROVED={ast.dump(ast.parse(s).body[0],include_attributes=False) for s in SNIPPETS}

class StripEarlyHooks(ast.NodeTransformer):
    def visit(self,node):
        if ast.dump(node,include_attributes=False) in APPROVED:
            return node.orelse if isinstance(node,ast.If) and node.orelse else None
        return super().visit(node)
