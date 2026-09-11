-- V5.13.4 fd37b56 schema for additive-migration regression tests. No production data.
CREATE TABLE autotrade_trades (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               signal_id INTEGER UNIQUE, symbol TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL,
               side TEXT NOT NULL DEFAULT 'LONG', position_side TEXT,
               margin_usdt REAL NOT NULL, leverage INTEGER NOT NULL, notional_usdt REAL NOT NULL,
               entry_signal_price REAL, entry_price REAL, qty REAL, expected_qty REAL,
               stop_price REAL, tp1_price REAL, tp2_price REAL, runner_price REAL,
               exit_profile TEXT, runner_fraction REAL, runner_target_pct REAL,
               tp1_hit INTEGER DEFAULT 0, partial_realized_pnl REAL DEFAULT 0,
               entry_order_id TEXT, entry_client_id TEXT,
               tp1_algo_id TEXT, tp1_client_id TEXT,
               tp2_algo_id TEXT, tp2_client_id TEXT,
               stop_algo_id TEXT, stop_client_id TEXT,
               opened_ts_ms INTEGER, closed_ts_ms INTEGER, close_reason TEXT, exit_price REAL,
               realized_pnl REAL DEFAULT 0, commission REAL DEFAULT 0, net_pnl REAL DEFAULT 0,
               manual_intervention INTEGER DEFAULT 0, last_error TEXT, updated_ts INTEGER NOT NULL
           );
CREATE TABLE autotrade_daily (
               local_date TEXT NOT NULL, scope TEXT NOT NULL, start_balance REAL NOT NULL, realized_net_pnl REAL DEFAULT 0,
               consecutive_stops INTEGER DEFAULT 0, locked INTEGER DEFAULT 0, lock_reason TEXT, cooldown_until_ts INTEGER DEFAULT 0,
               updated_ts INTEGER NOT NULL, PRIMARY KEY(local_date, scope)
           );
CREATE TABLE position_observer_state (
               symbol TEXT NOT NULL, position_side TEXT NOT NULL, direction TEXT NOT NULL,
               entry_price REAL NOT NULL, qty REAL NOT NULL, leverage INTEGER, source TEXT,
               zone TEXT, pending_zone TEXT, pending_since_ts INTEGER DEFAULT 0,
               profit_hits_json TEXT DEFAULT '[]', loss_hits_json TEXT DEFAULT '[]',
               last_roe REAL, last_unrealized_pnl REAL, active INTEGER DEFAULT 1, updated_ts INTEGER NOT NULL,
               PRIMARY KEY(symbol,position_side)
           );
CREATE TABLE premium_liquidity_snapshots (
            signal_id INTEGER NOT NULL,
            horizon_ms INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL,
            request_delay_ms INTEGER,
            reference_price REAL,
            mid_price REAL, best_bid REAL, best_ask REAL,
            bid_010 REAL, bid_025 REAL, bid_050 REAL, bid_100 REAL,
            ask_010 REAL, ask_025 REAL, ask_050 REAL, ask_100 REAL,
            ask_before_tp1 REAL,
            bid_ratio_025 REAL,
            largest_ask_wall_price REAL, largest_ask_wall_notional REAL,
            largest_ask_wall_distance_pct REAL, largest_ask_wall_ratio REAL,
            largest_bid_wall_price REAL, largest_bid_wall_notional REAL,
            largest_bid_wall_distance_pct REAL, largest_bid_wall_ratio REAL,
            aggressive_buy_speed_usdt_s REAL,
            ask025_clear_s REAL, tp1_clear_s REAL,
            wall_persisted INTEGER, wall_remaining_ratio REAL,
            wall_replenished INTEGER, wall_cancelled INTEGER,
            barrier_label TEXT, absorption_flag INTEGER,
            depth_levels INTEGER, ask_coverage_pct REAL, bid_coverage_pct REAL,
            ask025_vs_initial REAL, bid025_vs_initial REAL, tp1_ask_vs_initial REAL,
            bid_ratio_delta REAL, ask025_clear_vs_initial REAL, tp1_clear_vs_initial REAL,
            dynamic_score INTEGER, dynamic_state TEXT, dynamic_reason TEXT,
            updated_ts INTEGER NOT NULL, liquidity_v2_score INTEGER, liquidity_v2_state TEXT, liquidity_v2_reason TEXT, liquidity_core_score INTEGER, liquidity_core_state TEXT, liquidity_core_reason TEXT,
            PRIMARY KEY(signal_id, horizon_ms)
        );
CREATE TABLE premium_liquidity_transition_v3 (
               signal_id INTEGER PRIMARY KEY, finalized_ts_ms INTEGER NOT NULL,
               liq_state_5 TEXT, liq_state_15 TEXT, liq_state_30 TEXT,
               liq_score_5 INTEGER, liq_score_15 INTEGER, liq_score_30 INTEGER,
               oi_regime TEXT, barrier_30 TEXT, ask025_vs_initial_30 REAL, bid025_vs_initial_30 REAL,
               transition_state TEXT, reason TEXT, updated_ts INTEGER NOT NULL
           );
CREATE TABLE entry_stage_forward_shadow (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               symbol TEXT NOT NULL, episode_id INTEGER, stage TEXT NOT NULL, signal_id INTEGER,
               decision TEXT, created_ts_ms INTEGER NOT NULL, entry_age_s REAL, entry_price REAL NOT NULL,
               stop_price REAL, tp1_price REAL, tp2_price REAL,
               tp1_hit_s REAL, tp2_hit_s REAL, stop_hit_s REAL,
               be0_hit_s REAL, be10_hit_s REAL, be15_hit_s REAL,
               mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
               runner25_active_s REAL, runner25_exit_s REAL, runner25_exit_price REAL, runner25_peak REAL,
               runner30_active_s REAL, runner30_exit_s REAL, runner30_exit_price REAL, runner30_peak REAL,
               became_premium INTEGER DEFAULT 0, close60_price REAL, completed_60m INTEGER DEFAULT 0,
               current_outcome TEXT, current_return_pct REAL,
               be0_outcome TEXT, be0_return_pct REAL, be10_outcome TEXT, be10_return_pct REAL, be15_outcome TEXT, be15_return_pct REAL,
               late25_outcome TEXT, late25_return_pct REAL, late30_outcome TEXT, late30_return_pct REAL,
               fee_adjusted_current_pct REAL, fee_adjusted_be0_pct REAL, fee_adjusted_be10_pct REAL, fee_adjusted_be15_pct REAL,
               fee_adjusted_late25_pct REAL, fee_adjusted_late30_pct REAL,
               updated_ts INTEGER NOT NULL
           );
CREATE TABLE research_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            episode_id INTEGER,
            price REAL NOT NULL,
            score INTEGER,
            chg10 REAL, chg30 REAL, chg60 REAL, chg5 REAL, chg15 REAL,
            flow10 REAL, flow30 REAL, flow60 REAL,
            buy30 REAL, book_imbalance REAL, rel30 REAL, spread REAL,
            breakout INTEGER, gainer_rank INTEGER, rank_velocity REAL,
            compression_ratio REAL, dist15high_pct REAL,
            flow_eff30 REAL, flow_eff60 REAL, anchor_flow30 REAL,
            oi5 REAL, note TEXT
        , funding_rate_pct REAL, short_liq REAL, long_liq REAL, origin_signal_id INTEGER, oi_prev5 REAL, oi_accel5 REAL, oi_regime TEXT, btc30 REAL, price_accel10 REAL, flow_accel10 REAL, shadow_score INTEGER, shadow_label TEXT, gate_failures TEXT);
