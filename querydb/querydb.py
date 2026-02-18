#!/usr/bin/env python3
"""querydb: refresh /config/stations.list from PostgreSQL.

This tool supports two source view styles:

1) station_list_source-style (recommended)
   View exposes the final columns for stations.list:
     mp, rinex_id, x, y, z, rec_type, rec_ver, ant_type, ant_h, ant_e, ant_n

2) etat_antennes4-style (legacy)
   View exposes columns:
     mp, serial_code_world, ecef, receiver, version, antenne

Because stations.list is whitespace-separated, any *internal spaces* inside
REC_TYPE / REC_VER / ANT_TYPE are encoded as '|' in the output file.
The converters decode '|' back to spaces when writing RINEX headers.
"""

import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg2


FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
SPACE_ENC = "|"  # encode spaces inside tokens for stations.list


def env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default


def env_int(name: str, default: int) -> int:
    v = env(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ecef_xyz(ecef_value) -> tuple[str, str, str] | None:
    """Accepts text like 'x y z' or 'x,y,z' or already-3 columns."""
    if ecef_value is None:
        return None
    if isinstance(ecef_value, (list, tuple)) and len(ecef_value) >= 3:
        return str(ecef_value[0]), str(ecef_value[1]), str(ecef_value[2])
    s = str(ecef_value)
    nums = FLOAT_RE.findall(s)
    if len(nums) < 3:
        return None
    return nums[0], nums[1], nums[2]


def safe_ident(s: str, what: str) -> str:
    """Best-effort allowlist for identifiers/order-by snippets.

    This is not a full SQL parser, but it blocks obviously dangerous characters.
    The user fully controls QUERYDB_* anyway; this is a guardrail.
    """

    if not re.fullmatch(r"[A-Za-z0-9_\.\s,]+", s):
        raise ValueError(f"Invalid {what}: {s!r}")
    return s


def normalize_token(s: str | None, default: str = "") -> str:
    """Normalize a token for whitespace-separated stations.list.

    - trims
    - converts internal whitespace to SPACE_ENC (default '|')
    - keeps underscores intact (important: some receiver/antenna ids contain '_')
    """

    if s is None:
        return default
    s = str(s).strip()
    if s == "" or s.lower() == "null":
        return default

    # Stations.list is whitespace-separated; keep token single-field.
    s = re.sub(r"\s+", SPACE_ENC, s)

    # NOTE: literal "|" characters in DB values are not escaped.
    # If present, converters will decode them as spaces.

    return s


def format_float(v) -> str:
    if v is None:
        return ""
    try:
        # Preserve scientific notation if provided; psycopg2 may already return Decimal/float.
        return str(v).strip()
    except Exception:
        return ""


@dataclass
class Config:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str
    view: str
    where_sql: str | None
    order_by: str
    interval_s: int
    output: Path
    ant_h_default: str
    ant_e_default: str
    ant_n_default: str
    once: bool
    mode: str  # auto | station_list_source | etat_antennes4


def load_config() -> Config:
    host = env("QUERYDB_PGHOST", "host.docker.internal")
    port = env_int("QUERYDB_PGPORT", 5432)
    dbname = env("QUERYDB_PGDATABASE", "centipede")
    user = env("QUERYDB_PGUSER", "centipede_readonly")
    password = env("QUERYDB_PGPASSWORD", "") or ""
    sslmode = env("QUERYDB_PGSSLMODE", "prefer")

    view = env("QUERYDB_VIEW", "public.station_list_source")
    where_sql = env("QUERYDB_WHERE", None)
    order_by = env("QUERYDB_ORDER_BY", "mp")

    interval_s = env_int("QUERYDB_INTERVAL_SECONDS", 3600)
    output = Path(env("QUERYDB_OUTPUT", "/config/stations.list"))

    ant_h_default = env("QUERYDB_ANT_H_DEFAULT", "0.0")
    ant_e_default = env("QUERYDB_ANT_E_DEFAULT", "0.0")
    ant_n_default = env("QUERYDB_ANT_N_DEFAULT", "0.0")

    once = (env("QUERYDB_ONCE", "false").lower() == "true")

    # Source mode (optional)
    mode = env("QUERYDB_MODE", "auto") or "auto"
    mode = mode.lower()
    if mode not in {"auto", "station_list_source", "etat_antennes4"}:
        mode = "auto"

    # Guardrails on snippets used in SQL composition
    safe_ident(view, "QUERYDB_VIEW")
    safe_ident(order_by, "QUERYDB_ORDER_BY")

    if interval_s < 60:
        interval_s = 60

    return Config(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        view=view,
        where_sql=where_sql,
        order_by=order_by,
        interval_s=interval_s,
        output=output,
        ant_h_default=ant_h_default,
        ant_e_default=ant_e_default,
        ant_n_default=ant_n_default,
        once=once,
        mode=mode,
    )


def is_station_list_source_view(cfg: Config) -> bool:
    if cfg.mode == "station_list_source":
        return True
    if cfg.mode == "etat_antennes4":
        return False
    # auto
    return cfg.view.lower().endswith("station_list_source")


def build_sql(cfg: Config) -> str:
    where = f" WHERE {cfg.where_sql} " if cfg.where_sql else " "

    if is_station_list_source_view(cfg):
        return (
            "SELECT mp, rinex_id, x, y, z, rec_type, rec_ver, ant_type, ant_h, ant_e, ant_n "
            f"FROM {cfg.view}{where}ORDER BY {cfg.order_by};"
        )

    # Legacy etat_antennes4-like view
    return (
        "SELECT mp, serial_code_world, ecef, receiver, version, antenne "
        f"FROM {cfg.view}{where}ORDER BY {cfg.order_by};"
    )


def write_station_list(cfg: Config, rows: list[tuple]) -> int:
    ts = now_utc_iso()
    out = cfg.output
    out.parent.mkdir(parents=True, exist_ok=True)

    tmp = out.with_suffix(out.suffix + ".tmp")

    sql = build_sql(cfg)

    count = 0
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write("# stations.list (auto-generated)\n")
        f.write(f"# generated_at_utc={ts}\n")
        f.write(f"# source={cfg.view}\n")
        f.write(f"# mode={cfg.mode}\n")
        if cfg.where_sql:
            f.write(f"# where={cfg.where_sql}\n")
        f.write("#\n")
        f.write("# Token encoding: internal spaces in REC_TYPE/REC_VER/ANT_TYPE are written as '|'\n")
        f.write("# Decoding is handled by the converters when writing RINEX headers.\n")
        f.write("#\n")
        f.write("# <MOUNTPOINT> <RINEX_ID> <X> <Y> <Z> <REC_TYPE> <REC_VER> <ANT_TYPE> <ANT_H> <ANT_E> <ANT_N>\n")

        for row in rows:
            # New mode: view already provides final columns
            if len(row) >= 11 and is_station_list_source_view(cfg):
                (
                    mp,
                    rinex_id,
                    x,
                    y,
                    z,
                    rec_type,
                    rec_ver,
                    ant_type,
                    ant_h,
                    ant_e,
                    ant_n,
                ) = row[:11]

                mp_t = normalize_token(mp)
                rinex_id_t = normalize_token(rinex_id, mp_t)

                x_t, y_t, z_t = format_float(x), format_float(y), format_float(z)

                rec_type_t = normalize_token(rec_type, "UNKNOWN")
                rec_ver_t = normalize_token(rec_ver, "UNKNOWN")

                # ANT_TYPE is expected as "<antenna_id> <radome>" in DB view
                ant_type_t = normalize_token(ant_type, "NONE|NONE")

                ant_h_t = normalize_token(ant_h, cfg.ant_h_default)
                ant_e_t = normalize_token(ant_e, cfg.ant_e_default)
                ant_n_t = normalize_token(ant_n, cfg.ant_n_default)

                f.write(
                    f"{mp_t} {rinex_id_t} {x_t} {y_t} {z_t} {rec_type_t} {rec_ver_t} {ant_type_t} {ant_h_t} {ant_e_t} {ant_n_t}\n"
                )
                count += 1
                continue

            # Legacy mode: derive columns from etat_antennes4-like view
            if len(row) < 6:
                continue
            mp, rinex_id, ecef, receiver, version, antenna = row[:6]

            mp_t = normalize_token(mp)
            rinex_id_t = normalize_token(rinex_id, mp_t)

            xyz = parse_ecef_xyz(ecef)
            if xyz is None:
                x_t = y_t = z_t = ""
            else:
                x_t, y_t, z_t = xyz

            rec_type_t = normalize_token(receiver, "UNKNOWN")
            rec_ver_t = normalize_token(version, "UNKNOWN")

            # Never write ADVNULLANTENNA into stations.list
            ant_type_t = normalize_token(antenna, "NONE|NONE")

            ant_h_t = cfg.ant_h_default
            ant_e_t = cfg.ant_e_default
            ant_n_t = cfg.ant_n_default

            f.write(
                f"{mp_t} {rinex_id_t} {x_t} {y_t} {z_t} {rec_type_t} {rec_ver_t} {ant_type_t} {ant_h_t} {ant_e_t} {ant_n_t}\n"
            )
            count += 1

    tmp.replace(out)
    return count


def fetch_rows(cfg: Config) -> list[tuple]:
    dsn = (
        f"host={cfg.host} port={cfg.port} dbname={cfg.dbname} "
        f"user={cfg.user} password={cfg.password} sslmode={cfg.sslmode}"
    )
    sql = build_sql(cfg)

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def loop(cfg: Config) -> None:
    while True:
        try:
            rows = fetch_rows(cfg)
            n = write_station_list(cfg, rows)
            print(f"[{now_utc_iso()}] OK  rows={n} -> {cfg.output}")
        except Exception as e:
            print(f"[{now_utc_iso()}] ERR {type(e).__name__}: {e}", file=sys.stderr)

        if cfg.once:
            return

        time.sleep(cfg.interval_s)


def main() -> int:
    cfg = load_config()
    print(
        f"[{now_utc_iso()}] querydb start host={cfg.host}:{cfg.port} db={cfg.dbname} view={cfg.view} mode={cfg.mode} interval_s={cfg.interval_s} output={cfg.output}"
    )
    loop(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
