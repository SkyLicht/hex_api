import sqlite3
import numpy as np
import pandas as pd
from typing import Tuple, Iterable, Dict, List, Optional

class ECDFBetweenStations:
    STATIONS = (
        'PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT',
        'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING'
    )

    def __init__(self, db_path: str, line_name: str,
                 normalize_timestamps: bool = False,
                 flow_groups: Tuple[str, ...] = (
                     'PTH_INPUT','TOUCH_INSPECT','TOUCH_UP','ICT','FT1','FINAL_VI','FINAL_INSPECT','PACKING'
                 ),
                 repair_groups: Tuple[str, ...] = ('TUP_REPAIR','ICT_REPAIR','FT_REPAIR')):


        self.db_path = db_path
        self.line = line_name
        self.normalize = normalize_timestamps
        self.flow_groups = flow_groups
        self.repair_groups = repair_groups

    @staticmethod
    def _placeholders(n: int) -> str:
        return ",".join(["?"] * n)

    def _sql_pair(self,
                  stage_from: str,
                  stage_to: str,
                  start_dt: Optional[str],
                  end_dt: Optional[str],
                  anchor: str,
                  cap_minutes: Optional[int],
                  censor_flow_errors: bool,
                  censor_repairs: bool) -> Tuple[str, List]:
        if stage_from not in self.STATIONS or stage_to not in self.STATIONS:
            raise ValueError(f"stage_from/to deben estar en {self.STATIONS}")
        if anchor not in ("start","end","both"):
            raise ValueError("anchor debe ser 'start', 'end' o 'both'")

        # Campo timestamp / base
        if not self.normalize:
            ts_field = "collected_timestamp"
            base_cte = "records_table"
        else:
            ts_field = "ts19"
            base_cte = """
              (SELECT ppid, line_name, group_name, error_flag,
                      REPLACE(substr(collected_timestamp,1,19),'T',' ') AS ts19
               FROM records_table)
            """

        # WITH encadenado
        sql = f"""
        WITH
        base AS (SELECT * FROM {base_cte}),
        from_ev AS (
          SELECT ppid, MIN({ts_field}) AS t_from
          FROM base
          WHERE group_name=? AND line_name=? AND error_flag=0
          GROUP BY ppid
        ),
        to_ev AS (
          SELECT r.ppid, MIN(r.{ts_field}) AS t_to
          FROM base r
          JOIN from_ev f ON f.ppid=r.ppid
          WHERE r.group_name=? AND r.line_name=? AND r.error_flag=0
            AND r.{ts_field} > f.t_from
          GROUP BY r.ppid
        ),
        pairs AS (
          SELECT f.ppid, f.t_from, k.t_to
          FROM from_ev f JOIN to_ev k USING(ppid)
        )
        """
        params: List = [stage_from, self.line, stage_to, self.line]

        # CTEs de censura (opcionales)
        if censor_flow_errors:
            flow_ph = self._placeholders(len(self.flow_groups))
            sql += f""",
            ppids_with_errors AS (
              SELECT DISTINCT r.ppid
              FROM base r
              JOIN pairs p ON r.ppid = p.ppid
              WHERE r.group_name IN ({flow_ph})
                AND r.line_name = ?
                AND r.error_flag = 1
                AND r.{ts_field} BETWEEN p.t_from AND p.t_to
            )"""
            params += list(self.flow_groups) + [self.line]

        if censor_repairs:
            rep_ph = self._placeholders(len(self.repair_groups))
            sql += f""",
            ppids_with_repairs AS (
              SELECT DISTINCT r.ppid
              FROM base r
              JOIN pairs p ON r.ppid = p.ppid
              WHERE r.group_name IN ({rep_ph})
                AND r.line_name = ?
                AND r.{ts_field} BETWEEN p.t_from AND p.t_to
            )"""
            params += list(self.repair_groups) + [self.line]

        # SELECT final con filtros sobre pairs p
        sql += """
        SELECT
          p.ppid,
          p.t_from,
          p.t_to,
          CAST(ROUND((julianday(p.t_to) - julianday(p.t_from)) * 1440) AS INT) AS dwell_min
        FROM pairs p
        WHERE 1=1
        """

        # Ventana por anchor
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

        # Cap
        if cap_minutes is not None:
            sql += " AND (julianday(p.t_to) - julianday(p.t_from)) * 1440 BETWEEN 0 AND ?"
            params.append(int(cap_minutes))

        # Exclusiones por censura
        if censor_flow_errors:
            sql += " AND p.ppid NOT IN (SELECT ppid FROM ppids_with_errors)"
        if censor_repairs:
            sql += " AND p.ppid NOT IN (SELECT ppid FROM ppids_with_repairs)"

        sql += " ORDER BY dwell_min DESC"
        return sql, params

    # ------- API -------
    def durations(self, stage_from: str, stage_to: str,
                  start_dt: Optional[str] = None, end_dt: Optional[str] = None,
                  anchor: str = "start", cap_minutes: Optional[int] = None,
                  censor_flow_errors: bool = False, censor_repairs: bool = False) -> pd.DataFrame:
        sql, params = self._sql_pair(stage_from, stage_to, start_dt, end_dt,
                                     anchor, cap_minutes, censor_flow_errors, censor_repairs)
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn, params=params)

    @staticmethod
    def ecdf(durations_min: Iterable[int]) -> Tuple[callable, np.ndarray]:
        x = np.sort(np.asarray(list(durations_min), dtype=float))
        n = x.size
        if n == 0:
            raise ValueError("No hay duraciones para ECDF.")
        def F(t):
            t = np.asarray(t, dtype=float)
            return np.searchsorted(x, t, side="right") / n
        return F, x

    @staticmethod
    def sample_ecdf(durations_min: Iterable[int], grid: Iterable[float]) -> pd.DataFrame:
        F, _ = ECDFBetweenStations.ecdf(durations_min)
        t = np.asarray(list(grid), dtype=float)
        return pd.DataFrame({"t_min": t, "F(t)": F(t)})

    @staticmethod
    def percentiles(durations_min: Iterable[int],
                    probs: Iterable[float]=(0.5,0.9,0.95,0.99)) -> Dict[float,float]:
        x = np.sort(np.asarray(list(durations_min), dtype=float))
        n = x.size
        if n == 0:
            raise ValueError("No hay duraciones para percentiles.")
        out: Dict[float,float] = {}
        for p in probs:
            k = max(0, min(int(np.ceil(p*n))-1, n-1))
            out[p] = float(x[k])
        return out

    # Atajos
    def pth_to_packing(self, *args, **kwargs) -> pd.DataFrame:
        return self.durations('PTH_INPUT','PACKING', *args, **kwargs)

    def consecutive_pairs(self, start_dt: Optional[str]=None, end_dt: Optional[str]=None,
                          anchor: str="start", cap_minutes: Optional[int]=None,
                          censor_flow_errors: bool=False, censor_repairs: bool=False):
        out = {}
        for a,b in zip(self.STATIONS[:-1], self.STATIONS[1:]):
            out[(a,b)] = self.durations(a,b,start_dt,end_dt,anchor,cap_minutes,
                                        censor_flow_errors,censor_repairs)
        out[('PTH_INPUT','PACKING')] = self.pth_to_packing(start_dt,end_dt,anchor,cap_minutes,
                                                           censor_flow_errors,censor_repairs)
        return out




if __name__ == "__main__":
    m = ECDFBetweenStations("C:/Users/abrah/Desktop/sfc_db/lllll.db", "J01")


    # PTH→PACKING entre 08:00–14:00 anclado al inicio, con censura completa:

    dur = m.pth_to_packing(
        start_dt="2025-08-15 08:00:00",
        end_dt="2025-08-15 14:00:00",
        anchor="start",  # filtra por t_from en la ventana
        cap_minutes=1440,
        censor_flow_errors=True,
        censor_repairs=True
    )

    F, _ = m.ecdf(dur["dwell_min"])
    # print("F(120) =", float(F(120)))
    # print(m.percentiles(dur["dwell_min"]))

    # Window anchored at the START (ICT time) between 08:00–14:00
    dur_ict_ft1 = m.durations(
        stage_from="FINAL_INSPECT",
        stage_to="PACKING",
        start_dt="2025-08-15 09:00:00",
        end_dt="2025-08-15 10:00:00",
        anchor="start",  # filter by t_from (ICT)
        cap_minutes=1440,
        censor_flow_errors=True,  # optional censorship
        censor_repairs=True
    )
    F, _ = m.ecdf(dur_ict_ft1["dwell_min"])
    print("P(ICT→FT1 ≤ 60 min) =", float(F(60)))
    print("Percentiles:", m.percentiles(dur_ict_ft1["dwell_min"]))


