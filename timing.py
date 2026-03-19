from pytz import timezone

ET = timezone('America/New_York')

TIMES = {
    'premarket_snapshot':  '09:25',
    'health_check':        '09:28',
    'layer1_main_scan':    '09:30',
    'orb_5min_close':      '09:35',
    'orb_15min_close':     '09:45',
    'entry_window_open':   '09:45',
    'dead_zone_start':     '11:30',
    'dead_zone_end':       '14:30',
    'power_hour_start':    '15:00',
    'force_exit':          '15:45',
    'market_close':        '16:00',
}

MARKET_HOLIDAYS_2026 = [
    '2026-01-01',
    '2026-01-19',
    '2026-02-16',
    '2026-04-03',
    '2026-05-25',
    '2026-07-03',
    '2026-09-07',
    '2026-11-26',
    '2026-11-27',
    '2026-12-25',
]

ETF_WATCHLIST = [
    'QQQ',   # highest priority — best ORB backtests
    'SPY',   # broad market
    'IWM',   # small caps
    'USO',   # energy / oil
    'XLE',   # energy sector
    'XOP',   # oil & gas exploration
    'XBI',   # biotech
    'IBB',   # biotech large cap
    'GLD',   # gold
    'GDX',   # gold miners
    'XLK',   # tech sector
]

# Leveraged and inverse ETFs/ETPs that slip through Finviz's stock screener.
# These have volatility decay, gap/fade differently, and distort ORB signals.
# Excluded from the stock scanner; they are NOT eligible ETF watchlist entries either.
LEVERAGED_ETF_BLACKLIST = {
    # Broad index leveraged/inverse
    'UPRO', 'SPXL', 'SPXS', 'SDS', 'SSO', 'SH',
    # Nasdaq leveraged/inverse
    'TQQQ', 'SQQQ', 'QLD', 'QID',
    # Russell 2000 leveraged/inverse
    'TNA', 'TZA', 'URTY', 'SRTY', 'UWM', 'TWM',
    # Semiconductor leveraged/inverse
    'SOXL', 'SOXS',
    # Technology leveraged/inverse
    'TECL', 'TECS', 'BULZ', 'BERZ', 'FNGU', 'FNGD', 'WEBL', 'WEBS',
    # Biotech leveraged/inverse
    'LABU', 'LABD',
    # Energy leveraged/inverse
    'ERX', 'ERY', 'GUSH', 'DRIP',
    # Gold/miners leveraged/inverse
    'NUGT', 'DUST', 'JNUG', 'JDST',
    # VIX leveraged/inverse
    'UVIX', 'SVIX', 'UVXY', 'SVXY',
    # Crypto leveraged
    'BITX', 'ETHU', 'ETHT', 'MSTU', 'MSTX',
    # Single-stock leveraged ETFs
    'METU', 'METD',
    'NVDL', 'NVDS',
    'TSLL', 'TSLS',
    'AMDL', 'AMDS',
    'WDCX',
    # Regional/country leveraged
    'KORU', 'KORZ',   # 3x Korea
    'YINN', 'YANG',   # 3x China
    # Single-stock leveraged (additional)
    'BABX',           # 2x BABA
    # S&P leveraged ETN
    'SPYU',           # 4x S&P 500 ETN
}

SCAN_CONFIG = {
    'min_price':       10.0,    # sub-$15 stocks have wider spreads and erratic action
    'max_price':       100.0,    # keeps position sizing comfortable on an $8k account
    'min_gap_pct':     3.0,
    'max_gap_pct':     6.0,     # above 6% fade rate climbs sharply
    'min_volume':      500_000,
    # 5% — only stocks with real institutional premarket interest
    'min_pm_vol_pct':  0.05,
    'min_atr_pct':     1.5,     # ensures enough intraday range to hit 2R targets
    'atr_period':      14,      # more responsive than 20; industry standard
    'max_candidates':  10,       # forces higher quality, less decision fatigue
    'finviz_delay_s':  60,      # wait after 9:30 for Finviz to update
    'retry_attempts':  3,
    'retry_delay_s':   2,
}

# ETFs trade at higher prices, gap less, and have lower ATR% than individual stocks.
ETF_SCAN_CONFIG = {
    'min_price':       15.0,
    'max_price':       2000.0,  # QQQ/SPY/GLD can be $400–$700+
    'min_gap_pct':     1.0,     # 1% on SPY/QQQ is a meaningful overnight move
    'max_gap_pct':     5.0,
    'min_volume':      1_000_000,  # SIP volume from Tradier — cuts out thin ETF days
    'min_pm_vol_pct':  0.008,   # 0.8% of avg daily vol — ETFs trade huge avg volume
    'min_atr_pct':     0.7,     # ETFs have lower ATR% (SPY ~1.4%, QQQ ~1.7%)
    'atr_period':      14,
}
