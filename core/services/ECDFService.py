# utils_ecdf.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import bisect

# Tip: tu interfaz ya existe
# from your_project.db import SQLiteReadOnlyConnection

STATIONS = (
    'PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT',
    'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING'
)

FLOW_GROUPS = STATIONS
REPAIR_GROUPS = ('TUP_REPAIR','ICT_REPAIR','FT_REPAIR')

class ECDFService:
    """
    Servicio para:
      - Construir pares (t_from, t_to) entre estaciones para un PPID (primer evento to > from).
      - Aplicar filtros de ventana (start_dt/end_dt) con anchor 'start'|'end'|'both'.
      - Censurar opcionalmente por errores (error_flag=1) y reparaciones en el intervalo.
      - Devolver duraciones (min), ECDF, percentiles y batch minutes.
    """
    def __init__(self, database: "SQLiteReadOnlyConnection", line_name: str):
        self.db = database
        self.line = line_name

    # ---------------- SQL builder ----------------
    def _sql_pairs(self,
                   stage_from: str,
                   stage_to: str) -> str:
        if stage_from not in STATIONS or stage_to not in STATIONS:
            raise ValueError(f"stage_from/to deben pertenecer a {STATIONS}")

        # CTEs: from_ev, to_ev, pairs (sin filtros de ventana aquí)
        return f"""
        WITH
        base AS (SELECT * FROM records_table),
        from_ev AS (
          SELECT ppid, MIN(collected_timestamp) AS t_from
          FROM base
          WHERE group_name=? AND line_name=? AND error_flag=0
          GROUP BY ppid
        ),
        to_ev AS (
          SELECT r.ppid, MIN(r.collected_timestamp) AS t_to
          FROM base r
          JOIN from_ev f ON f.ppid=r.ppid
          WHERE r.group_name=? AND r.line_name=? AND r.error_flag=0
            AND r.collected_timestamp > f.t_from
          GROUP BY r.ppid
        ),
        pairs AS (
          SELECT f.ppid, f.t_from, k.t_to
          FROM from_ev f JOIN to_ev k USING(ppid)
        )
        SELECT
          p.ppid,
          p.t_from,
          p.t_to,
          CAST(ROUND((julianday(p.t_to) - julianday(p.t_from)) * 1440) AS INT) AS dwell_min
        FROM pairs p
        WHERE 1=1
        """

    def get_durations(self,
                      stage_from: str = "FINAL_INSPECT",
                      stage_to: str   = "PACKING",
                      start_dt: Optional[str] = None,
                      end_dt:   Optional[str] = None,
                      anchor:   str = "start",         # 'start'|'end'|'both'
                      cap_minutes: Optional[int] = 1440,
                      censor_flow_errors: bool = True,
                      censor_repairs:     bool = True) -> List[Dict[str, Any]]:
        """
        Devuelve lista de dicts: {ppid, t_from, t_to, dwell_min}
        """
        sql = self._sql_pairs(stage_from, stage_to)
        params: List[Any] = [stage_from, self.line, stage_to, self.line]

        # Ventana sobre SELECT final (pares p)
        if anchor not in ("start","end","both"):
            raise ValueError("anchor debe ser 'start'|'end'|'both'")

        if anchor in ("start","both"):
            if start_dt:
                sql += " AND p.t_from >= ?"
                params.append(start_dt)
            if end_dt:
                sql += " AND p.t_from <= ?"
                params.append(end_dt)

        if anchor in ("end","both"):
            if start_dt:
                sql += " AND p.t_to >= ?"
                params.append(start_dt)
            if end_dt:
                sql += " AND p.t_to <= ?"
                params.append(end_dt)

        if cap_minutes is not None:
            sql += " AND (julianday(p.t_to) - julianday(p.t_from)) * 1440 BETWEEN 0 AND ?"
            params.append(int(cap_minutes))

        # Censura opcional (uniones contra pairs p)
        if censor_flow_errors:
            flow_in = ",".join(["?"] * len(FLOW_GROUPS))
            sql = f"""
            WITH base AS (SELECT * FROM records_table),
                 from_ev AS (
                   SELECT ppid, MIN(collected_timestamp) AS t_from
                   FROM base WHERE group_name=? AND line_name=? AND error_flag=0
                   GROUP BY ppid
                 ),
                 to_ev AS (
                   SELECT r.ppid, MIN(r.collected_timestamp) AS t_to
                   FROM base r JOIN from_ev f ON f.ppid=r.ppid
                   WHERE r.group_name=? AND r.line_name=? AND r.error_flag=0
                     AND r.collected_timestamp > f.t_from
                   GROUP BY r.ppid
                 ),
                 pairs AS (SELECT f.ppid, f.t_from, k.t_to FROM from_ev f JOIN to_ev k USING(ppid)),
                 ppids_with_errors AS (
                   SELECT DISTINCT r.ppid
                   FROM base r JOIN pairs p ON r.ppid=p.ppid
                   WHERE r.group_name IN ({flow_in})
                     AND r.line_name=? AND r.error_flag=1
                     AND r.collected_timestamp BETWEEN p.t_from AND p.t_to
                 )
            SELECT
              p.ppid, p.t_from, p.t_to,
              CAST(ROUND((julianday(p.t_to)-julianday(p.t_from))*1440) AS INT) AS dwell_min
            FROM pairs p
            WHERE p.ppid NOT IN (SELECT ppid FROM ppids_with_errors)
            """
            params = [stage_from, self.line, stage_to, self.line] + list(FLOW_GROUPS) + [self.line]
            # vuelve a aplicar ventana/cap (mismo orden que antes)
            if anchor in ("start","both"):
                if start_dt:
                    sql += " AND p.t_from >= ?"; params.append(start_dt)
                if end_dt:
                    sql += " AND p.t_from <= ?"; params.append(end_dt)
            if anchor in ("end","both"):
                if start_dt:
                    sql += " AND p.t_to >= ?"; params.append(start_dt)
                if end_dt:
                    sql += " AND p.t_to <= ?"; params.append(end_dt)
            if cap_minutes is not None:
                sql += " AND (julianday(p.t_to) - julianday(p.t_from)) * 1440 BETWEEN 0 AND ?"
                params.append(int(cap_minutes))

        if censor_repairs:
            rep_in = ",".join(["?"] * len(REPAIR_GROUPS))
            sql = f"""
            WITH base AS (SELECT * FROM records_table),
                 from_ev AS (
                   SELECT ppid, MIN(collected_timestamp) AS t_from
                   FROM base WHERE group_name=? AND line_name=? AND error_flag=0
                   GROUP BY ppid
                 ),
                 to_ev AS (
                   SELECT r.ppid, MIN(r.collected_timestamp) AS t_to
                   FROM base r JOIN from_ev f ON f.ppid=r.ppid
                   WHERE r.group_name=? AND r.line_name=? AND r.error_flag=0
                     AND r.collected_timestamp > f.t_from
                   GROUP BY r.ppid
                 ),
                 pairs AS (SELECT f.ppid, f.t_from, k.t_to FROM from_ev f JOIN to_ev k USING(ppid)),
                 ppids_with_repairs AS (
                   SELECT DISTINCT r.ppid
                   FROM base r JOIN pairs p ON r.ppid=p.ppid
                   WHERE r.group_name IN ({rep_in})
                     AND r.line_name=? 
                     AND r.collected_timestamp BETWEEN p.t_from AND p.t_to
                 )
            SELECT
              p.ppid, p.t_from, p.t_to,
              CAST(ROUND((julianday(p.t_to)-julianday(p.t_from))*1440) AS INT) AS dwell_min
            FROM pairs p
            WHERE p.ppid NOT IN (SELECT ppid FROM ppids_with_repairs)
            """
            params = [stage_from, self.line, stage_to, self.line] + list(REPAIR_GROUPS) + [self.line]
            if anchor in ("start","both"):
                if start_dt:
                    sql += " AND p.t_from >= ?"; params.append(start_dt)
                if end_dt:
                    sql += " AND p.t_from <= ?"; params.append(end_dt)
            if anchor in ("end","both"):
                if start_dt:
                    sql += " AND p.t_to >= ?"; params.append(start_dt)
                if end_dt:
                    sql += " AND p.t_to <= ?"; params.append(end_dt)
            if cap_minutes is not None:
                sql += " AND (julianday(p.t_to) - julianday(p.t_from)) * 1440 BETWEEN 0 AND ?"
                params.append(int(cap_minutes))

        sql += " ORDER BY dwell_min DESC"

        rows = self.db.execute_query(sql, tuple(params))  # -> List[Dict]
        return rows or []

    # ---------------- ECDF & percentiles ----------------
    @staticmethod
    def ecdf_sample(durations_min: List[int],
                    grid_step: int = 10,
                    grid_max: Optional[int] = None) -> Dict[str, Any]:
        """
        Devuelve grid y F(t) para JSON: {"t": [...], "F": [...]}
        """
        xs = sorted(int(x) for x in durations_min if x is not None)
        n = len(xs)
        if n == 0:
            return {"t": [], "F": []}
        if grid_max is None:
            grid_max = max(xs)
        t_grid = list(range(0, int(grid_max)+1, int(grid_step)))
        F_vals: List[float] = []
        for t in t_grid:
            # bisect_right = # de elementos <= t
            k = bisect.bisect_right(xs, t)
            F_vals.append(k / n)
        return {"t": t_grid, "F": F_vals}

    @staticmethod
    def percentiles(durations_min: List[int],
                    probs: Tuple[float, ...] = (0.5, 0.9, 0.95, 0.99)) -> Dict[str, float]:
        xs = sorted(int(x) for x in durations_min if x is not None)
        n = len(xs)
        out: Dict[str, float] = {}
        if n == 0:
            return {str(p): float("nan") for p in probs}
        for p in probs:
            idx = max(0, min((int(p * n + 0.999999) - 1), n - 1))  # ceil(p*n)-1
            out[str(p)] = float(xs[idx])
        return out

    # -------------- detección de “hiding/batch” --------------
    @staticmethod
    def _floor_minute(ts: str) -> datetime:
        # espera 'YYYY-MM-DD HH:MM:SS'
        dt = datetime.fromisoformat(ts.replace("T"," ")[:19])
        return dt.replace(second=0, microsecond=0)

    def detect_batch_minutes(self,
                             pairs: List[Dict[str, Any]],
                             count_threshold: int = 10,
                             median_threshold_min: int = 60) -> List[Dict[str, Any]]:
        """
        Agrupa por minuto de t_to, calcula conteo y mediana(dwell).
        Devuelve lista de minutos con count>=X y mediana>=Y.
        """
        buckets: Dict[datetime, List[int]] = {}
        for r in pairs:
            t_to = r.get("t_to")
            dmin = r.get("dwell_min")
            if t_to is None or dmin is None:
                continue
            key = self._floor_minute(str(t_to))
            buckets.setdefault(key, []).append(int(dmin))

        out: List[Dict[str, Any]] = []
        for k, arr in buckets.items():
            arr.sort()
            c = len(arr)
            # mediana discreta: promedio de centrales si par
            if c % 2 == 1:
                med = float(arr[c//2])
            else:
                med = (arr[c//2 - 1] + arr[c//2]) / 2.0
            if c >= count_threshold and med >= median_threshold_min:
                out.append({
                    "minute": k.isoformat(sep=" "),
                    "count": c,
                    "median_dwell_min": med
                })
        out.sort(key=lambda x: x["minute"])
        return out

    # -------------- análisis end-to-end --------------

    def get_ecdf(
        self,
        stage_from: str = "FINAL_INSPECT",
        stage_to: str   = "PACKING",
        date: Optional[str] = None,                # "YYYY-MM-DD"
        start_dt: Optional[str] = None,            # "YYYY-MM-DD HH:MM:SS"
        end_dt: Optional[str] = None,              # "YYYY-MM-DD HH:MM:SS"
        anchor: str = "start",                     # 'start' | 'end' | 'both'
        cap_minutes: int = 1440,
        censor_flow_errors: bool = True,
        censor_repairs: bool = True,
        grid_step: int = 10,
        grid_max: Optional[int] = None,
        eval_at: Optional[List[int]] = None        # optional times (min) where you want F(t)
    ) -> Dict[str, Any]:
        """
        Returns ECDF info only:
          {
            line, stage_from, stage_to, window, n,
            percentiles: {p50, p90, p95, p99},
            grid: {"t":[...], "F":[...]},
            support: {"min":..., "max":...},
            F_at: [{"t": m, "F": val}, ...]    # if eval_at provided
          }
        """
        if date:
            if start_dt or end_dt:
                raise ValueError("Use either 'date' or ('start_dt','end_dt'), not both.")
            start_dt = f"{date} 00:00:00"
            end_dt   = f"{date} 23:59:59"

        # Pull pairs & durations
        pairs = self.get_durations(
            stage_from=stage_from,
            stage_to=stage_to,
            start_dt=start_dt,
            end_dt=end_dt,
            anchor=anchor,
            cap_minutes=cap_minutes,
            censor_flow_errors=censor_flow_errors,
            censor_repairs=censor_repairs,
        )
        if not pairs:
            return {
                "line": self.line,
                "stage_from": stage_from,
                "stage_to": stage_to,
                "window": {"anchor": anchor, "start_dt": start_dt, "end_dt": end_dt},
                "n": 0,
                "percentiles": {"p50": None, "p90": None, "p95": None, "p99": None},
                "grid": {"t": [], "F": []},
                "support": {"min": None, "max": None},
            }

        durations = sorted(int(r["dwell_min"]) for r in pairs if r.get("dwell_min") is not None)
        n = len(durations)
        dmin, dmax = durations[0], durations[-1]

        # Grid
        if grid_max is None:
            grid_max = dmax
        grid_max = int(max(0, grid_max))
        grid_step = max(1, int(grid_step))
        t_grid = list(range(0, grid_max + 1, grid_step))
        F_vals: List[float] = []
        for t in t_grid:
            k = bisect.bisect_right(durations, t)  # # ≤ t
            F_vals.append(k / n)

        # Percentiles
        def pct(p: float) -> float:
            idx = max(0, min(int(p * n + 0.999999) - 1, n - 1))  # ceil(p*n)-1
            return float(durations[idx])
        pcts = {"p50": pct(0.5), "p90": pct(0.9), "p95": pct(0.95), "p99": pct(0.99)}

        # Optional F at specific times
        F_at = None
        if eval_at:
            F_at = []
            for m in eval_at:
                m = int(m)
                k = bisect.bisect_right(durations, m)
                F_at.append({"t": m, "F": k / n})

        return {
            "line": self.line,
            "stage_from": stage_from,
            "stage_to": stage_to,
            "window": {"anchor": anchor, "start_dt": start_dt, "end_dt": end_dt},
            "n": n,
            "percentiles": pcts,
            "grid": {"t": t_grid, "F": F_vals},
            "support": {"min": dmin, "max": dmax},
            **({"F_at": F_at} if F_at is not None else {}),
        }