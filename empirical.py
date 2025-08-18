import sqlite3
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Iterable, Tuple, Dict, List, Optional

@dataclass
class PackingECDF:
    """
    Herramienta para estimar la distribución empírica (ECDF) y PMF de los tiempos
    PTH_INPUT → PACKING, aplicando *censura*:
      - Excluye PPIDs con error_flag=1 en cualquier grupo del flujo entre t_in y t_out.
      - Excluye PPIDs con registros de reparación entre t_in y t_out.
    """
    db_path: str
    line_name: str
    cap_minutes: int = 1440
    # Grupos que se consideran en el flujo para revisar errores
    flow_groups: Tuple[str, ...] = (
        'PTH_INPUT', 'TOUCH_INSPECT', 'TOUCH_UP', 'ICT',
        'FT1', 'FINAL_VI', 'FINAL_INSPECT', 'PACKING'
    )
    # Grupos de reparación a excluir si aparecen entre t_in y t_out
    repair_groups: Tuple[str, ...] = ('TUP_REPAIR', 'ICT_REPAIR', 'FT_REPAIR')
    # Si tus timestamps vienen como 'YYYY-MM-DDTHH:MM:SS.sssZ', activa normalización
    normalize_timestamps: bool = False

    # ------------------ SQL builder ------------------
    def _placeholders(self, n: int) -> str:
        return ",".join(["?"] * n)

    def _sql_durations_censored(self) -> Tuple[str, List]:
        """
        Construye el SQL con CTEs y parámetros posicionales (sqlite3).
        Retorna (sql, params_list).
        """
        params: List = []

        # IN (...) de flow_groups y repair_groups
        flow_ph = self._placeholders(len(self.flow_groups))
        repair_ph = self._placeholders(len(self.repair_groups))

        if not self.normalize_timestamps:
            # Versión directa (julianday sobre collected_timestamp)
            sql = f"""
            WITH
              params AS (
                SELECT ? AS line, ? AS d_from, ? AS d_to, ? AS cap_minutes
              ),
              pth AS (
                SELECT ppid, MIN(collected_timestamp) AS t_in
                FROM records_table, params
                WHERE group_name='PTH_INPUT'
                  AND line_name = line
                  AND error_flag=0
                  AND date(collected_timestamp) BETWEEN d_from AND d_to
                GROUP BY ppid
              ),
              pack AS (
                SELECT r.ppid, MIN(r.collected_timestamp) AS t_out
                FROM records_table r
                JOIN pth p ON p.ppid = r.ppid
                JOIN params pr
                WHERE r.group_name='PACKING'
                  AND r.line_name  = pr.line
                  AND r.error_flag = 0
                  AND r.collected_timestamp > p.t_in
                GROUP BY r.ppid
              ),
              ppids_with_errors AS (
                SELECT DISTINCT r.ppid
                FROM records_table r
                JOIN pth p   ON r.ppid = p.ppid
                JOIN pack k  ON r.ppid = k.ppid
                JOIN params pr
                WHERE r.group_name IN ({flow_ph})
                  AND r.line_name = pr.line
                  AND r.error_flag = 1
                  AND r.collected_timestamp BETWEEN p.t_in AND k.t_out
              ),
              ppids_with_repairs AS (
                SELECT DISTINCT r.ppid
                FROM records_table r
                JOIN pth p   ON r.ppid = p.ppid
                JOIN pack k  ON r.ppid = k.ppid
                JOIN params pr
                WHERE r.group_name IN ({repair_ph})
                  AND r.line_name = pr.line
                  AND r.collected_timestamp BETWEEN p.t_in AND k.t_out
              )
            SELECT
              p.ppid,
              p.t_in,
              k.t_out,
              CAST(ROUND((julianday(k.t_out) - julianday(p.t_in)) * 1440) AS INT) AS dwell_min
            FROM pth p
            JOIN pack k USING (ppid)
            LEFT JOIN ppids_with_errors e ON p.ppid = e.ppid
            LEFT JOIN ppids_with_repairs r ON p.ppid = r.ppid
            WHERE k.t_out IS NOT NULL
              AND e.ppid IS NULL
              AND r.ppid IS NULL
              AND (julianday(k.t_out) - julianday(p.t_in)) * 1440 BETWEEN 0 AND (SELECT cap_minutes FROM params)
            ORDER BY dwell_min DESC
            """
            params.extend([self.line_name, None, None, self.cap_minutes])
            # d_from, d_to se pondrán luego (para no duplicar lógica)
        else:
            # Normaliza timestamps a 'YYYY-MM-DD HH:MM:SS' (quita 'T' y milisegundos)
            sql = f"""
            WITH
              params AS (
                SELECT ? AS line, ? AS d_from, ? AS d_to, ? AS cap_minutes
              ),
              base AS (
                SELECT
                  ppid, line_name, group_name, error_flag,
                  REPLACE(substr(collected_timestamp,1,19),'T',' ') AS ts19
                FROM records_table
              ),
              pth AS (
                SELECT ppid, MIN(ts19) AS t_in
                FROM base, params
                WHERE group_name='PTH_INPUT'
                  AND line_name = line
                  AND error_flag=0
                  AND date(ts19) BETWEEN d_from AND d_to
                GROUP BY ppid
              ),
              pack AS (
                SELECT r.ppid, MIN(r.ts19) AS t_out
                FROM base r
                JOIN pth p ON p.ppid = r.ppid
                JOIN params pr
                WHERE r.group_name='PACKING'
                  AND r.line_name  = pr.line
                  AND r.error_flag = 0
                  AND r.ts19 > p.t_in
                GROUP BY r.ppid
              ),
              ppids_with_errors AS (
                SELECT DISTINCT r.ppid
                FROM base r
                JOIN pth p  ON r.ppid = p.ppid
                JOIN pack k ON r.ppid = k.ppid
                JOIN params pr
                WHERE r.group_name IN ({flow_ph})
                  AND r.line_name = pr.line
                  AND r.error_flag = 1
                  AND r.ts19 BETWEEN p.t_in AND k.t_out
              ),
              ppids_with_repairs AS (
                SELECT DISTINCT r.ppid
                FROM base r
                JOIN pth p  ON r.ppid = p.ppid
                JOIN pack k ON r.ppid = k.ppid
                JOIN params pr
                WHERE r.group_name IN ({repair_ph})
                  AND r.line_name = pr.line
                  AND r.ts19 BETWEEN p.t_in AND k.t_out
              )
            SELECT
              p.ppid,
              p.t_in,
              k.t_out,
              CAST(ROUND((julianday(k.t_out) - julianday(p.t_in)) * 1440) AS INT) AS dwell_min
            FROM pth p
            JOIN pack k USING (ppid)
            LEFT JOIN ppids_with_errors e ON p.ppid = e.ppid
            LEFT JOIN ppids_with_repairs r ON p.ppid = r.ppid
            WHERE k.t_out IS NOT NULL
              AND e.ppid IS NULL
              AND r.ppid IS NULL
              AND (julianday(k.t_out) - julianday(p.t_in)) * 1440 BETWEEN 0 AND (SELECT cap_minutes FROM params)
            ORDER BY dwell_min DESC
            """
            params.extend([self.line_name, None, None, self.cap_minutes])

        # Añade los valores del IN (...)
        params.extend(self.flow_groups)
        params.extend(self.repair_groups)

        return sql, params

    # ------------------ API pública ------------------
    def get_durations(
        self,
        d_from: str,
        d_to: str,
        as_dataframe: bool = True,
    ) -> pd.DataFrame:
        """
        Obtiene duraciones censuradas (query provisto), retorna columnas:
        [ppid, t_in, t_out, dwell_min]
        """
        sql, params = self._sql_durations_censored()

        # Inserta fechas en la lista de parámetros en las posiciones correctas
        # (line, d_from, d_to, cap_minutes, ...IN lists...)
        params[1] = d_from
        params[2] = d_to

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=params)

        if not as_dataframe:
            return df.to_records(index=False)
        return df

    @staticmethod
    def build_pmf(dwell_minutes: Iterable[int], cap_minutes: Optional[int] = None) -> Tuple[np.ndarray, Dict]:
        """
        Construye PMF f[k] con soporte 0..cap (por defecto usa max(dwell, cap_minutes) seguro).
        Devuelve (pmf, info_dict con n y percentiles).
        """
        arr = np.asarray(list(dwell_minutes), dtype=int)
        if arr.size == 0:
            raise ValueError("No hay duraciones para construir la PMF.")

        cap = int(cap_minutes) if cap_minutes is not None else max(int(arr.max()), 0)
        counts = np.bincount(arr.clip(min=0, max=cap), minlength=cap + 1)
        pmf = counts / counts.sum()

        xs = np.sort(arr)
        def q(p):
            k = max(0, min(int(np.ceil(p * len(xs))) - 1, len(xs) - 1))
            return float(xs[k])

        info = {"n": int(arr.size), "p50": q(0.50), "p90": q(0.90), "p95": q(0.95), "p99": q(0.99)}
        return pmf, info

    @staticmethod
    def build_ecdf(dwell_minutes: Iterable[int]):
        """
        Devuelve una función F(t) (ECDF empírica) y el vector ordenado.
        """
        x = np.sort(np.asarray(list(dwell_minutes), dtype=float))
        n = x.size
        def F(t):
            t = np.asarray(t, dtype=float)
            return np.searchsorted(x, t, side="right") / n
        return F, x

    def save_pmf_to_sqlite(self, pmf: np.ndarray, table_name: str = "dwell_pmf_minute_full") -> None:
        """
        Guarda la PMF en SQLite como (k_minute INTEGER PRIMARY KEY, prob REAL).
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}(
                k_minute INTEGER PRIMARY KEY,
                prob     REAL NOT NULL
            )""")
            cur.execute(f"DELETE FROM {table_name}")
            cur.executemany(
                f"INSERT INTO {table_name}(k_minute, prob) VALUES(?,?)",
                [(int(k), float(p)) for k, p in enumerate(pmf)]
            )
            conn.commit()

        # --- helpers de serie por minuto ---

    @staticmethod
    def _minute_grid(day_str: str) -> pd.DatetimeIndex:
        start = pd.to_datetime(f"{day_str} 00:00:00")
        end = pd.to_datetime(f"{day_str} 23:59:00")
        return pd.date_range(start, end, freq="1min")

    def _arrivals_series(self, day_str: str, max_k: int) -> pd.Series:
        day0 = pd.to_datetime(f"{day_str} 00:00:00")
        start = (day0 - pd.Timedelta(minutes=max_k)).strftime("%Y-%m-%d %H:%M:%S")
        end = f"{day_str} 23:59:59"
        sql = """
              SELECT datetime((strftime('%s', collected_timestamp) / 60) * 60, 'unixepoch') AS t_min,
                     COUNT(*)                                                               AS a
              FROM records_table
              WHERE line_name = ? \
                AND group_name = 'PTH_INPUT' \
                AND error_flag = 0
                AND collected_timestamp BETWEEN ? AND ?
              GROUP BY 1 \
              """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=(self.line_name, start, end))
        idx_ext = pd.date_range(day0 - pd.Timedelta(minutes=max_k),
                                f"{day_str} 23:59:00", freq="1min")
        s = pd.Series(0, index=idx_ext, dtype=int)
        if not df.empty:
            df["t_min"] = pd.to_datetime(df["t_min"])
            s.loc[df["t_min"].values] = df["a"].to_numpy(dtype=int)
        return s

    def _actual_first_pack_series(self, day_str: str) -> pd.Series:
        sql = """
              WITH pth_first AS (SELECT ppid, MIN(collected_timestamp) AS t_in \
                                 FROM records_table \
                                 WHERE line_name = ? \
                                   AND group_name = 'PTH_INPUT' \
                                   AND error_flag = 0 \
                                 GROUP BY ppid),
                   pack_first AS (SELECT p.ppid, MIN(r.collected_timestamp) AS t_out \
                                  FROM pth_first p \
                                           JOIN records_table r \
                                                ON r.ppid = p.ppid \
                                                    AND r.group_name = 'PACKING' \
                                                    AND r.line_name = ? \
                                                    AND r.error_flag = 0 \
                                                    AND r.collected_timestamp > p.t_in \
                                  GROUP BY p.ppid)
              SELECT datetime((strftime('%s', t_out) / 60) * 60, 'unixepoch') AS t_min,
                     COUNT(*)                                                 AS actual_1min
              FROM pack_first
              WHERE t_out IS NOT NULL \
                AND date(t_out) = ?
              GROUP BY 1 \
              """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=(self.line_name, self.line_name, day_str))
        idx_day = self._minute_grid(day_str)
        s = pd.Series(0, index=idx_day, dtype=int)
        if not df.empty:
            df["t_min"] = pd.to_datetime(df["t_min"])
            s.loc[df["t_min"].values] = df["actual_1min"].to_numpy(dtype=int)
        return s

    @staticmethod
    def _crop_and_renormalize_pmf(pmf: np.ndarray, cap_k: int) -> np.ndarray:
        cap_k = int(cap_k)
        cap_k = max(0, min(cap_k, len(pmf) - 1))
        pmf2 = pmf[:cap_k + 1].copy()
        s = pmf2.sum()
        if s > 0 and abs(s - 1.0) > 1e-12:
            pmf2 /= s
        return pmf2

    def expected_for_day(self, day_str: str, pmf: np.ndarray,
                         cap_k: Optional[int] = None,
                         with_bands: bool = False) -> pd.DataFrame:
        """
        Calcula expected por minuto y acumulado para 'day_str'.
        - cap_k: recorta el kernel y renormaliza (ej. 480).
        - with_bands: añade bandas 95% (σ minuto y acumulado).
        Devuelve DataFrame con timepoint, expected_1min, expected_cum, actual_1min, actual_cum, gap_cum,
        y opcionalmente expected_1min_lo95/hi95, expected_cum_lo95/hi95.
        """
        kernel = pmf if cap_k is None else self._crop_and_renormalize_pmf(pmf, cap_k)
        K = len(kernel) - 1

        # Arribos extendidos (carry-over)
        A_ext = self._arrivals_series(day_str, K).to_numpy(dtype=float)
        # Convolución vectorizada
        exp_full = np.convolve(A_ext, kernel, mode="full")

        # Recorta al tramo del día
        idx_day = self._minute_grid(day_str)
        start = K
        stop = K + len(idx_day)
        expected_1min = exp_full[start:stop]
        expected_cum = np.cumsum(expected_1min)

        # Reales (primer PACKING por PPID)
        actual_1min = self._actual_first_pack_series(day_str).to_numpy(dtype=float)
        actual_cum = np.cumsum(actual_1min)
        gap_cum = actual_cum - expected_cum

        out = pd.DataFrame({
            "timepoint": idx_day,
            "expected_1min": np.round(expected_1min, 3),
            "expected_cum": np.round(expected_cum, 3),
            "actual_1min": actual_1min.astype(int),
            "actual_cum": actual_cum.astype(int),
            "gap_cum": np.round(gap_cum, 3),
        })

        if with_bands:
            # Varianza por minuto: sum A * p*(1-p)
            var_terms = np.convolve(A_ext, kernel * (1.0 - kernel), mode="full")[start:stop]
            sigma_1 = np.sqrt(np.maximum(var_terms, 0.0))
            lo1 = np.maximum(0.0, expected_1min - 1.96 * sigma_1)
            hi1 = expected_1min + 1.96 * sigma_1
            out["expected_1min_lo95"] = np.round(lo1, 3)
            out["expected_1min_hi95"] = np.round(hi1, 3)

            # Varianza acumulada (Poisson-binomial): sum A * F*(1-F)
            F = np.cumsum(kernel)
            var_kernel_cum = F * (1.0 - F)
            var_cum_terms = np.convolve(A_ext, var_kernel_cum, mode="full")[start:stop]
            sigma_c = np.sqrt(np.maximum(var_cum_terms, 0.0))
            out["expected_cum_lo95"] = np.round(expected_cum - 1.96 * sigma_c, 3)
            out["expected_cum_hi95"] = np.round(expected_cum + 1.96 * sigma_c, 3)

        return out



if __name__ == "__main__":


    # ------------------ EJEMPLO DE USO ------------------
    # (Cambia las rutas/fechas/línea a tu caso)
    DB_PATH = "C:/Users/abrah/Desktop/sfc_db/lllll.db"  # <-- CAMBIA
    LINE    = "J01"                        # <-- CAMBIA

    model = PackingECDF(
        db_path=DB_PATH,
        line_name=LINE,
        cap_minutes=1440,                  # soporte 24h para no perder cola
        normalize_timestamps=False         # pon True si tus timestamps traen 'T' o milisegundos problemáticos
    )

    # 1) Duraciones con censura aplicada (tu query)
    dur_df = model.get_durations(d_from="2025-08-10", d_to="2025-08-14")
    print(dur_df.head())

    # 2) PMF y percentiles
    pmf, info = model.build_pmf(dur_df["dwell_min"].to_numpy(), cap_minutes=model.cap_minutes)
    print("[PMF] n={n}  p50={p50}  p90={p90}  p95={p95}  p99={p99}".format(**info))
    print("sum(pmf)={:.6f}, soporte k=0..{} min".format(pmf.sum(), len(pmf)-1))

    # 3) (Opcional) Guardar PMF en SQLite para usarla en el pronóstico por minuto
    # model.save_pmf_to_sqlite(pmf, table_name="dwell_pmf_minute_full")

    # 4) (Opcional) ECDF empírica como función
    F, x_sorted = model.build_ecdf(dur_df["dwell_min"].to_numpy())
    print("Ejemplo: F(120) = prob de empacar en ≤120 min =", float(F(120)))


    # Ya tienes pmf e info (p99=416). Cap operativo recomendado ~480 min.
    cap_oper = 480

    df_day = model.expected_for_day(
        day_str="2025-08-14",
        pmf=pmf,
        cap_k=cap_oper,       # recorta a 480 y renormaliza
        with_bands=True       # opcional: bandas 95%
    )


    print(df_day.head(8).to_string(index=False))
    print(df_day.tail(8).to_string(index=False))