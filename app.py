# ============================================================
# 부동산 올인원 봇 v19 — exact_jibun 우선 전유공용면적 보정판
# ============================================================
import streamlit as st
import pandas as pd
import requests
import xmltodict
import re
import time
from io import BytesIO

DONG_API_KEY  = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

API_PATHS = {
    "아파트_매매":       "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "아파트_전월세":      "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "아파트분양권_매매":  "RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
    "오피스텔_매매":      "RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "오피스텔_전월세":    "RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "연립/다세대_매매":   "RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "연립/다세대_전월세": "RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "단독/다가구_매매":   "RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "단독/다가구_전월세": "RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    "상업/업무용_매매":   "RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
    "공장 및 창고_매매":  "RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade",
    "토지_매매":          "RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade",
}

_BASE     = "http://apis.data.go.kr/1613000/BldRgstHubService"
URL_RECAP = f"{_BASE}/getBrRecapTitleInfo"
URL_TITLE = f"{_BASE}/getBrTitleInfo"
URL_EXPOS = f"{_BASE}/getBrExposPubuseAreaInfo"
URL_FLOOR = f"{_BASE}/getBrFlrOulnInfo"
URL_ATCH  = f"{_BASE}/getBrAtchJibunInfo"

# ─────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────
def safe_val(val, default="-"):
    if val is None:
        return default
    s = str(val).strip()
    return default if s in ("", "None", "nan") else s

def fmt_date(d):
    s = safe_val(d)
    return f"{s[:4]}.{s[4:6]}.{s[6:]}" if (len(s) == 8 and s.isdigit()) else s

def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return 0.0

def parse_address(addr):
    parts = addr.strip().split()
    if not parts:
        return None, None, None, None
    plat_gb = "0"
    if "산" in parts:
        plat_gb = "1"
        parts.remove("산")
    bun, ji = "0000", "0000"
    last = parts[-1]
    if any(c.isdigit() for c in last):
        if "-" in last:
            b, j = last.split("-", 1)
            bun = "".join(filter(str.isdigit, b)).zfill(4) or "0000"
            ji  = "".join(filter(str.isdigit, j)).zfill(4) or "0000"
        else:
            bun = "".join(filter(str.isdigit, last)).zfill(4) or "0000"
        parts.pop()
    return " ".join(parts), plat_gb, bun, ji

def parse_platplc(platplc):
    s = str(platplc).strip()
    if not s or s in ("None", "nan", "-"):
        return None, None, None
    s = re.sub(r'[번지]+\s*$', '', s).strip()
    pgb = "1" if re.search(r'\s산\s*\d', s) else "0"
    m = re.search(r'(\d+)(?:-(\d+))?\s*$', s)
    if not m:
        return None, None, None
    return pgb, str(m.group(1)).zfill(4), (str(m.group(2)).zfill(4) if m.group(2) else "0000")

def normalize_dong_text(v):
    return re.sub(r"[^A-Za-z0-9가-힣]", "", str(v)).replace("제", "").upper()

def normalize_ho_text(v):
    return re.sub(r"[^A-Za-z0-9가-힣]", "", str(v)).replace("제", "").upper()

def get_bjdong_code(search_term):
    url = (
        f"https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
        f"?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=10&type=json&locatadd_nm={search_term}"
    )
    try:
        data = requests.get(url, timeout=10).json()
        if data.get("StanReginCd"):
            rows   = data["StanReginCd"][1]["row"]
            active = [r for r in rows if r["sido_cd"] != "" and r["sgg_cd"] != ""]
            if active:
                rc = active[0]["region_cd"]
                return rc[:5], rc[5:10], active[0]["locatadd_nm"]
    except:
        pass
    return None, None, None

def match_dong(target, dong_nm, bld_nm):
    if not target:
        return True
    t = normalize_dong_text(target).replace("동", "")
    d = normalize_dong_text(dong_nm).replace("동", "")
    b = normalize_dong_text(bld_nm)
    if not d and not b:
        return False
    if t and d and t == d:
        return True
    if t and f"{t}동" in b:
        return True
    nums = re.findall(r"\d+", t)
    if nums:
        n = nums[-1]
        short = str(int(n) % 100)
        if d in (n, short, f"주{n}", f"주{short}"):
            return True
        if f"{n}동" in b:
            return True
    return False

def strict_match_dong_only(target, dong_nm):
    if not target:
        return True
    t = normalize_dong_text(target).replace("동", "")
    d = normalize_dong_text(dong_nm).replace("동", "")
    if not d:
        return False
    nums_t = re.findall(r"\d+", t)
    nums_d = re.findall(r"\d+", d)
    if nums_t and nums_d:
        return nums_t[-1] == nums_d[-1]
    return t == d

def match_ho(target, ho_nm):
    if not target:
        return True
    t = normalize_ho_text(target).replace("호", "")
    h = normalize_ho_text(ho_nm).replace("호", "")
    if t == h:
        return True
    tn = re.findall(r"\d+", t)
    hn = re.findall(r"\d+", h)
    if tn and hn:
        try:
            return int(tn[-1]) == int(hn[-1])
        except:
            return tn[-1] == hn[-1]
    return False

# ─────────────────────────────────────────────────────────────
# API 호출
# ─────────────────────────────────────────────────────────────
def fetch_bld_api(endpoint, sgg_cd, bjdong_cd, plat_gb, bun, ji, max_pages=50):
    all_items = []
    for page in range(1, max_pages + 1):
        url = (
            f"{endpoint}?serviceKey={MOLIT_API_KEY}"
            f"&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}"
            f"&platGbCd={plat_gb}&bun={bun}&ji={ji}"
            f"&numOfRows=1000&pageNo={page}"
        )
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=15)
                if res.text.strip().startswith("<"):
                    xml_data = xmltodict.parse(res.content)
                    if "OpenAPI_ServiceResponse" not in xml_data:
                        break
            except:
                time.sleep(0.5)
        body  = xml_data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", []) if body.get("items") else []
        if isinstance(items, dict):
            items = [items]
        if not items:
            break
        all_items.extend(items)
        if page * 1000 >= int(body.get("totalCount", 0)):
            break
    return all_items

def get_all_jibun(sgg_cd, bjdong_cd, plat_gb, bun, ji):
    result = [(sgg_cd, bjdong_cd, plat_gb, bun, ji)]
    items  = fetch_bld_api(URL_ATCH, sgg_cd, bjdong_cd, plat_gb, bun, ji, max_pages=5)
    for item in items:
        c = (
            str(item.get("atchSigunguCd", sgg_cd)).zfill(5),
            str(item.get("atchBjdongCd",  bjdong_cd)).zfill(5),
            str(item.get("atchPlatGbCd",  "0")),
            str(item.get("atchBun", "0")).zfill(4),
            str(item.get("atchJi",  "0")).zfill(4),
        )
        if c not in result:
            result.append(c)
    return result

# ─────────────────────────────────────────────────────────────
# 건축물대장 메인
# ─────────────────────────────────────────────────────────────
def get_building_ledger(sgg_cd, bjdong_cd, plat_gb, bun, ji, target_dong="", target_ho=""):
    status     = st.empty()
    plat_cands = ["3", "2", "0"] if plat_gb != "1" else ["1"]
    pk_map     = {}

    def restore(r, key):
        v = r.get(key, "")
        if not v or str(v).strip() in ("", "None", "nan"):
            pk = r.get("mgmBldrgstPk")
            if pk and pk in pk_map:
                return pk_map[pk].get("dong" if key == "dongNm" else "bld", "")
        return v

    status.info("🗺️ 단지 필지 구성 파악 중...")
    all_jibun = get_all_jibun(sgg_cd, bjdong_cd, plat_gb, bun, ji)

    # STEP 1 표제부
    status.info("📋 표제부 수집 중...")
    raw_titles = []
    for (js, jb, jp, jbun, jji) in all_jibun:
        for p_gb in plat_cands:
            items = fetch_bld_api(URL_TITLE, js, jb, p_gb, jbun, jji, max_pages=10)
            valid = [x for x in items if to_float(x.get("totArea", "0")) > 0]
            if valid:
                raw_titles.extend(valid)
                break

    seen, unique_titles = set(), []
    for item in raw_titles:
        pk = item.get("mgmBldrgstPk")
        if pk not in seen:
            seen.add(pk)
            unique_titles.append(item)

    df_titles = pd.DataFrame(unique_titles) if unique_titles else pd.DataFrame()

    for item in unique_titles:
        pk = item.get("mgmBldrgstPk")
        if pk:
            pk_map[pk] = {"dong": item.get("dongNm", ""), "bld": item.get("bldNm", "")}

    target_pks = set()
    if target_dong and not df_titles.empty:
        for _, row in df_titles.iterrows():
            if match_dong(target_dong, row.get("dongNm", ""), row.get("bldNm", "")):
                pk = row.get("mgmBldrgstPk")
                if pk:
                    target_pks.add(pk)

    target_exact_jibun = []
    if not df_titles.empty:
        seen_j = set()
        for _, row in df_titles.iterrows():
            if target_dong and not match_dong(target_dong, row.get("dongNm", ""), row.get("bldNm", "")):
                continue
            for plc_field in ["platPlc", "newPlatPlc"]:
                pgb, bn, jn = parse_platplc(row.get(plc_field, ""))
                if bn:
                    key = (str(sgg_cd).zfill(5), str(bjdong_cd).zfill(5), str(pgb), str(bn).zfill(4), str(jn).zfill(4))
                    if key not in seen_j:
                        seen_j.add(key)
                        target_exact_jibun.append(key)
                    break

    # STEP 2 총괄표제부
    status.info("🏢 총괄표제부 수집 중...")
    df_recap = pd.DataFrame()
    for p_gb in ["0", "2", "3", "1"]:
        items = fetch_bld_api(URL_RECAP, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=2)
        valid = [x for x in items if to_float(x.get("totArea", "0")) > 0]
        if valid:
            df_recap = pd.DataFrame(valid)
            break

    # STEP 3 전유공용면적
    status.info("🏠 전유공용면적 수집 중...")
    df_expos        = pd.DataFrame()
    is_missing_area = False
    expos_debug_log = []

    def row_jibun_key(r):
        pgb = safe_val(r.get("platGbCd"), "")
        bn  = safe_val(r.get("bun"), "")
        jn  = safe_val(r.get("ji"), "")
        if bn == "-" or not bn:
            p2, b2, j2 = parse_platplc(r.get("platPlc", ""))
            if b2:
                pgb = p2
                bn = b2
                jn = j2
        if bn in ("", "-"):
            return None
        return (
            str(sgg_cd).zfill(5),
            str(bjdong_cd).zfill(5),
            str(pgb if pgb not in ("", "-") else "0"),
            str(bn).zfill(4),
            str(jn if jn not in ("", "-") else "0000").zfill(4),
        )

    def finalize_expos_df(df_src, source_label=""):
        nonlocal is_missing_area, expos_debug_log

        if df_src is None or df_src.empty:
            return pd.DataFrame()

        df_src = df_src.copy()
        df_src["dongNm"] = df_src.apply(lambda r: restore(r, "dongNm"), axis=1)
        df_src["bldNm"]  = df_src.apply(lambda r: restore(r, "bldNm"), axis=1)
        df_src["_jibun_key"] = df_src.apply(row_jibun_key, axis=1)

        df_ho = df_src[df_src.apply(
            lambda r: match_ho(target_ho, r.get("hoNm", "")), axis=1
        )]

        expos_debug_log.append({
            "단계": f"{source_label} hoNm필터",
            "건수": len(df_ho),
            "매칭된hoNm": df_ho["hoNm"].tolist()[:5] if not df_ho.empty else [],
            "매칭된dongNm": df_ho["dongNm"].tolist()[:5] if not df_ho.empty else [],
            "매칭된지번": [str(x) for x in df_ho["_jibun_key"].tolist()[:5]] if not df_ho.empty else [],
        })

        if df_ho.empty:
            return pd.DataFrame()

        if target_dong:
            df_strict = df_ho[df_ho.apply(
                lambda r: strict_match_dong_only(target_dong, r.get("dongNm", "")),
                axis=1
            )]
            expos_debug_log.append({
                "단계": f"{source_label} strict_dong필터",
                "건수": len(df_strict),
                "매칭된dongNm": df_strict["dongNm"].tolist()[:5] if not df_strict.empty else [],
            })
            if not df_strict.empty:
                df_ho = df_strict
            else:
                exact_set = set(target_exact_jibun or [])
                df_exact = df_ho[df_ho["_jibun_key"].isin(exact_set)] if exact_set else pd.DataFrame()

                expos_debug_log.append({
                    "단계": f"{source_label} exact_jibun필터",
                    "건수": len(df_exact),
                    "exact_jibun": [str(x) for x in list(exact_set)[:5]],
                    "매칭된dongNm": df_exact["dongNm"].tolist()[:5] if not df_exact.empty else [],
                    "매칭된지번": [str(x) for x in df_exact["_jibun_key"].tolist()[:5]] if not df_exact.empty else [],
                })

                if not df_exact.empty:
                    df_ho = df_exact
                else:
                    df_dong = df_ho[df_ho.apply(
                        lambda r: match_dong(target_dong, r.get("dongNm", ""), r.get("bldNm", "")),
                        axis=1
                    )]
                    expos_debug_log.append({
                        "단계": f"{source_label} dong필터",
                        "target_dong": target_dong,
                        "건수": len(df_dong),
                        "매칭된dongNm": df_dong["dongNm"].tolist()[:5] if not df_dong.empty else [],
                        "매칭된bldNm": df_dong["bldNm"].tolist()[:5] if not df_dong.empty else [],
                    })
                    if df_dong.empty:
                        return pd.DataFrame()
                    df_ho = df_dong

        if "area" not in df_ho.columns:
            df_ho["area"] = "0"
            is_missing_area = True

        return df_ho.drop(columns=["_jibun_key"], errors="ignore")

    if target_ho:
        found_by_exact = False

        if target_exact_jibun:
            for (js, jb, jp, jbun, jji) in target_exact_jibun:
                if found_by_exact:
                    break
                for p_gb in [jp] + [x for x in ["2", "3", "0", "1"] if x != jp]:
                    items = fetch_bld_api(URL_EXPOS, js, jb, p_gb, jbun, jji, max_pages=20)
                    if not items:
                        continue

                    tmp = pd.DataFrame(items)
                    expos_debug_log.append({
                        "방식": "exact_jibun 직접조회",
                        "지번": f"bun={jbun} ji={jji} platGb={p_gb}",
                        "수집건수": len(tmp),
                        "hoNm샘플": tmp["hoNm"].tolist()[:5] if "hoNm" in tmp.columns else [],
                    })

                    result_df = finalize_expos_df(
                        tmp,
                        source_label=f"exact_jibun {jbun}-{jji} platGb={p_gb}"
                    )
                    if not result_df.empty:
                        df_expos = result_df
                        expos_debug_log.append({
                            "결과": "exact_jibun에서 동/호 또는 지번/호 일치 데이터 발견",
                            "건수": len(df_expos),
                        })
                        found_by_exact = True
                        break

        if not found_by_exact:
            expos_debug_log.append({"exact_jibun조회": "동/호 또는 지번/호 일치 결과 없음 → 전체 수집으로 전환"})
            all_expos_raw = []

            for (js, jb, jp, jbun, jji) in all_jibun:
                for p_gb in [jp] + [x for x in ["2", "3", "0", "1"] if x != jp]:
                    items = fetch_bld_api(URL_EXPOS, js, jb, p_gb, jbun, jji, max_pages=20)
                    if items:
                        expos_debug_log.append({
                            "방식": "전체수집",
                            "지번": f"bun={jbun} ji={jji} platGb={p_gb}",
                            "수집건수": len(items),
                            "hoNm샘플": [x.get("hoNm", "") for x in items[:5]],
                            "pk샘플": [x.get("mgmBldrgstPk", "") for x in items[:3]],
                        })
                        all_expos_raw.extend(items)

            if not all_expos_raw:
                expos_debug_log.append({"결과": "전체 조회 0건"})
            else:
                seen_e, unique_expos = set(), []
                for item in all_expos_raw:
                    key = (
                        item.get("mgmBldrgstPk"),
                        item.get("hoNm"),
                        item.get("exposPubuseGbCdNm"),
                        item.get("area"),
                    )
                    if key not in seen_e:
                        seen_e.add(key)
                        unique_expos.append(item)

                df_all = pd.DataFrame(unique_expos)
                expos_debug_log.append({"전체수집(중복제거)": len(df_all)})

                result_df = finalize_expos_df(df_all, source_label="전체수집")
                if not result_df.empty:
                    df_expos = result_df
                else:
                    expos_debug_log.append({
                        "최종결과": "동/호 또는 exact_jibun/호가 동시에 일치하는 전유공용면적 없음"
                    })

    # STEP 4 층별개요
    status.info("🪜 층별개요 수집 중...")
    df_floor    = pd.DataFrame()
    found       = False
    floor_order = target_exact_jibun + [j for j in all_jibun if (
        str(j[0]).zfill(5), str(j[1]).zfill(5), str(j[2]), str(j[3]).zfill(4), str(j[4]).zfill(4)
    ) not in set(target_exact_jibun)]

    for item in floor_order:
        js, jb, jp, jbun, jji = item
        if found:
            break
        for p_gb in [jp] + [x for x in plat_cands if x != jp]:
            items = fetch_bld_api(URL_FLOOR, js, jb, p_gb, jbun, jji, max_pages=20)
            if not items:
                continue
            tmp = pd.DataFrame(items)
            tmp["dongNm"] = tmp.apply(lambda r: restore(r, "dongNm"), axis=1)
            tmp["bldNm"]  = tmp.apply(lambda r: restore(r, "bldNm"), axis=1)

            if target_pks and "mgmBldrgstPk" in tmp.columns:
                tmp = tmp[tmp["mgmBldrgstPk"].isin(target_pks)]
            elif target_dong:
                tmp = tmp[tmp.apply(
                    lambda r: match_dong(target_dong, r.get("dongNm", ""), r.get("bldNm", "")),
                    axis=1
                )]

            if not tmp.empty:
                tmp = tmp.copy()
                tmp["_n"] = pd.to_numeric(tmp.get("flrNo", pd.Series(dtype=str)), errors="coerce").fillna(-99)
                df_floor = tmp.sort_values("_n", ascending=False).drop(columns=["_n"])
                found = True
                break

    status.empty()
    return (
        df_recap, df_titles, df_expos, df_floor,
        is_missing_area, len(all_jibun), target_exact_jibun, expos_debug_log
    )

# ─────────────────────────────────────────────────────────────
# 실거래가
# ─────────────────────────────────────────────────────────────
def get_sigungu_code(sigungu_name, dong_name):
    search_term = dong_name.strip() if dong_name.strip() else sigungu_name.strip()
    url = (
        f"https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
        f"?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=500&type=json&locatadd_nm={search_term}"
    )
    try:
        res = requests.get(url, timeout=10)
        if not res.text.strip():
            return None, None
        data = res.json()
        if data.get("StanReginCd"):
            rows   = data["StanReginCd"][1]["row"]
            active = [r for r in rows if r["sido_cd"] != "" and r["sgg_cd"] != ""]
            for region in active:
                if sigungu_name.strip() in region["locatadd_nm"]:
                    return region["region_cd"][:5], region["locatadd_nm"]
        return None, None
    except:
        return None, None

def get_real_estate_data(sigungu_code, start_month, end_month, dong_name, prop_type, trans_type):
    dict_key = f"{prop_type}_{trans_type}"
    if dict_key not in API_PATHS:
        st.warning(f"⚠️ '{prop_type} {trans_type}' 조합은 지원하지 않습니다.")
        return pd.DataFrame()

    base_url = f"http://apis.data.go.kr/1613000/{API_PATHS[dict_key]}"
    try:
        month_list = pd.date_range(
            pd.to_datetime(start_month, format="%Y%m"),
            pd.to_datetime(end_month, format="%Y%m"),
            freq="MS"
        ).strftime("%Y%m").tolist()
    except:
        st.error("조회 기간 형식 오류 (YYYYMM)")
        return pd.DataFrame()

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, ym in enumerate(month_list):
        status_text.text(f"⏳ {ym} 조회 중... ({i+1}/{len(month_list)})")
        progress_bar.progress((i + 1) / len(month_list))
        url = (
            f"{base_url}?serviceKey={MOLIT_API_KEY}"
            f"&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={ym}"
        )
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if not res.text.strip().startswith("<"):
                break
            xml_data = xmltodict.parse(res.content)
            if "OpenAPI_ServiceResponse" in xml_data:
                break
            if xml_data.get("response", {}).get("header", {}).get("resultCode") not in ["00", "0", "200", "000"]:
                continue
            items = xml_data.get("response", {}).get("body", {}).get("items")
            if items and "item" in items:
                il = items["item"]
                if isinstance(il, dict):
                    il = [il]
                all_data.append(pd.DataFrame(il))
        except:
            continue
        time.sleep(0.3)

    status_text.empty()
    progress_bar.empty()

    if not all_data:
        st.warning("거래 내역이 없거나 조회가 중단되었습니다.")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    if dong_name.strip():
        df = df[df["umdNm"].str.contains(dong_name.strip(), na=False)]
    if df.empty:
        st.warning(f"'{dong_name}' 거래 내역 없음")
        return pd.DataFrame()

    df = df.rename(columns={
        "dealYear":"년","dealMonth":"월","dealDay":"일","umdNm":"법정동","jibun":"지번",
        "aptNm":"건물명","offiNm":"건물명","mviNm":"건물명","bldgNm":"건물명",
        "rletTypeNm":"건물유형","rletTpNm":"건물유형","buildingType":"건물유형",
        "purpsRgnNm":"용도지역","prpsRgnNm":"용도지역","landUse":"용도지역",
        "excluUseAr":"전용면적","area":"계약면적","dealArea":"거래면적",
        "bldgMarea":"건물면적","blgMarea":"건물면적","bldgArea":"건물면적","buildingAr":"건물면적",
        "plArea":"대지면적","platArea":"대지면적","totArea":"연면적","plottageAr":"대지면적",
        "dealAmount":"거래금액","deposit":"보증금","monthlyRent":"월세",
        "floor":"층","flr":"층","jimok":"지목","buildYear":"건축년도",
        "reqGbn":"거래유형","dealingGbn":"거래유형","cnclYmd":"계약취소일","cdealDay":"계약취소일",
        "estbDvsnNm":"중개사소재지","estateAgentSggNm":"중개사소재지","buildingUse":"건물주용도",
        "buyerGbn":"매수자","slerGbn":"매도자","shareDealingType":"지분거래여부","sggNm":"시군구",
    })

    if "법정동" in df.columns and "지번" in df.columns:
        df["소재지"] = df["법정동"] + " " + df["지번"].fillna("").astype(str).str.strip()
    elif "법정동" in df.columns:
        df["소재지"] = df["법정동"]

    if all(x in df.columns for x in ["년", "월", "일"]):
        df["계약일"] = (
            df["년"].astype(str) + "-" +
            df["월"].astype(str).str.zfill(2) + "-" +
            df["일"].astype(str).str.zfill(2)
        )

    if trans_type == "매매" and "거래금액" in df.columns:
        area_col = next((c for c in ["전용면적","건물면적","연면적","거래면적","대지면적","계약면적"] if c in df.columns), None)
        if area_col:
            def pyeong(row):
                try:
                    p = int(str(row["거래금액"]).replace(",", ""))
                    a = float(str(row[area_col]).replace(",", ""))
                    if a <= 0:
                        return ""
                    pp = int(p / (a / 3.3058))
                    uk, man = pp // 10000, pp % 10000
                    return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{pp}만원"
                except:
                    return ""
            df["평당가격"] = df.apply(pyeong, axis=1)

    disp = [
        "계약일","소재지","건물유형","용도지역","건물주용도","건물명","건축년도",
        "대지면적","건물면적","연면적","전용면적","층","거래금액","평당가격",
        "매수자","매도자","지분거래여부","거래유형","중개사소재지","계약취소일",
    ]
    df = df[[c for c in disp if c in df.columns]].copy()

    def fmt_money(v):
        if pd.isna(v):
            return ""
        try:
            p = int(str(v).replace(",", ""))
            uk, man = p // 10000, p % 10000
            return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{p}만원"
        except:
            return v

    for col in ["거래금액", "보증금"]:
        if col in df.columns:
            df[col] = df[col].apply(fmt_money)

    if "계약일" in df.columns:
        df = df.sort_values("계약일", ascending=False)

    return df

# ─────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 건축물대장 조회"])

with tab1:
    now = pd.Timestamp.now()
    with st.form("form_trade"):
        c1, c2 = st.columns(2)
        with c1:
            prop_t = st.selectbox("매물 종류", [
                "아파트","아파트분양권","오피스텔","연립/다세대",
                "단독/다가구","상업/업무용","공장 및 창고","토지"
            ])
        with c2:
            tran_t = st.selectbox("거래 종류", ["매매", "전월세"])
        c3, c4, c5, c6 = st.columns(4)
        with c3:
            sgg_nm = st.text_input("시/군/구", value="서초구")
        with c4:
            dn_nm = st.text_input("법정동 (빈칸=구 전체)")
        with c5:
            s_mon = st.text_input("시작월 YYYYMM", value=(now - pd.DateOffset(months=1)).strftime("%Y%m"))
        with c6:
            e_mon = st.text_input("종료월 YYYYMM", value=now.strftime("%Y%m"))
        sub1 = st.form_submit_button("🔍 실거래가 조회")

    if sub1:
        if not sgg_nm:
            st.warning("시/군/구를 입력하세요.")
        else:
            code, loc = get_sigungu_code(sgg_nm, dn_nm)
            if code:
                st.success(f"✅ {loc} ({code})")
                rdf = get_real_estate_data(code, s_mon, e_mon, dn_nm, prop_t, tran_t)
                if not rdf.empty:
                    rdf.index = range(1, len(rdf) + 1)
                    st.dataframe(rdf, use_container_width=True)
                    buf = BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter") as wr:
                        rdf.to_excel(wr, index=True, index_label="순번", sheet_name="실거래가")
                    st.download_button("📥 엑셀 다운로드", buf.getvalue(), "실거래가.xlsx")
            else:
                st.error("지역을 찾을 수 없습니다.")

with tab2:
    st.subheader("📋 건축물대장 종합 조회")
    st.caption("💡 전유공용면적은 exact_jibun 우선 검증")

    with st.form("form_bld"):
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            addr_in = st.text_input("지번 주소 (필수)", placeholder="예: 상도동 450 / 상도동 363-164")
        with c2:
            dong_in = st.text_input("동 (선택)", placeholder="예: 106")
        with c3:
            ho_in = st.text_input("호수 (선택)", placeholder="예: 201")
        sub2 = st.form_submit_button("🔍 건축물대장 열람")

    if sub2 and addr_in:
        region_term, plat_gb, bun, ji = parse_address(addr_in)
        if not region_term:
            st.warning("주소 형식을 확인해주세요.")
            st.stop()

        sgg_cd, bjdong_cd, full_loc = get_bjdong_code(region_term)
        if not sgg_cd:
            st.error("지역 코드를 찾을 수 없습니다.")
            st.stop()

        st.success(f"✅ {full_loc}  본번:{bun}  부번:{ji}")

        (
            df_recap, df_titles, df_expos, df_floor,
            is_missing_area, jibun_cnt, exact_jibun, expos_debug_log
        ) = get_building_ledger(sgg_cd, bjdong_cd, plat_gb, bun, ji, dong_in, ho_in)

        with st.expander("🔍 디버그 정보 (조회 안 될 때 확인)", expanded=False):
            st.write(f"**탐색 필지 수:** {jibun_cnt}개")
            if exact_jibun:
                st.write("**target_dong 실제 지번:**")
                for j in exact_jibun:
                    st.code(f"sigunguCd={j[0]}, bjdongCd={j[1]}, platGbCd={j[2]}, bun={j[3]}, ji={j[4]}")
            if not df_titles.empty and dong_in:
                matched = df_titles[df_titles.apply(
                    lambda r: match_dong(dong_in, r.get("dongNm", ""), r.get("bldNm", "")),
                    axis=1
                )]
                pk_list = matched["mgmBldrgstPk"].tolist() if not matched.empty and "mgmBldrgstPk" in matched.columns else []
                st.write(f"**표제부 동 매칭:** {len(matched)}건 | **target_pks:** {pk_list}")
                if not matched.empty:
                    cols = [c for c in ["dongNm", "bldNm", "platPlc", "mgmBldrgstPk"] if c in matched.columns]
                    st.dataframe(matched[cols].head(5))
            if ho_in and expos_debug_log:
                st.write("**전유공용면적 STEP 3 로그:**")
                for log in expos_debug_log:
                    st.json(log)

        if df_recap.empty and df_titles.empty:
            st.error("🚨 조회 결과 없음. 지번 주소를 다시 확인해주세요.")
            st.stop()

        if not df_recap.empty:
            r = df_recap.iloc[0]
            st.markdown("---")
            st.markdown("### 🏢 총괄표제부")
            st.markdown(f"**📍 {safe_val(r.get('bldNm'), '명칭없음')}**  `{safe_val(r.get('platPlc', r.get('newPlatPlc')))}`")

        if not df_titles.empty:
            st.markdown("---")
            st.markdown("### 📄 표제부 — 전체 동 목록")
            sum_cols = {
                "bldNm":"건물명","dongNm":"동명칭","platPlc":"대지위치",
                "mainPurpsCdNm":"주용도","strctCdNm":"주구조",
                "grndFlrCnt":"지상층","ugrndFlrCnt":"지하층",
                "heit":"높이(m)","totArea":"연면적(㎡)","hhldCnt":"세대수",
                "useAprDay":"사용승인일","violBldYn":"위반건축물",
            }
            ex = {k: v for k, v in sum_cols.items() if k in df_titles.columns}
            df_show = df_titles[list(ex.keys())].rename(columns=ex).copy()
            if "사용승인일" in df_show.columns:
                df_show["사용승인일"] = df_show["사용승인일"].apply(fmt_date)
            if "위반건축물" in df_show.columns:
                df_show["위반건축물"] = df_show["위반건축물"].apply(lambda x: "⚠️ 위반" if str(x).strip() == "1" else "정상")
            df_show.index = range(1, len(df_show) + 1)
            st.caption(f"총 {len(df_show)}개 동 조회됨")
            st.dataframe(df_show, use_container_width=True)

        if ho_in:
            st.markdown("---")
            if df_expos.empty:
                st.markdown(f"### 🚪 전유공용면적 — {dong_in + '동 ' if dong_in else ''}{ho_in}호")
                st.warning(
                    "⚠️ 요청한 동/호와 정확히 일치하는 전유공용면적 데이터를 찾을 수 없습니다.\n\n"
                    "- 이번 버전은 exact_jibun 일치도 함께 검사합니다\n"
                    "- 디버그 정보의 exact_jibun필터 로그를 확인하세요"
                )
            else:
                first_row = df_expos.iloc[0]
                actual_dong = safe_val(first_row.get("dongNm"), dong_in + "동" if dong_in else "")
                actual_ho   = safe_val(first_row.get("hoNm"), ho_in + "호")
                st.markdown(f"### 🚪 전유공용면적 — {actual_dong} {actual_ho}")

                pks = df_expos["mgmBldrgstPk"].unique() if "mgmBldrgstPk" in df_expos.columns else [None]
                for pk in pks[:3]:
                    grp = df_expos[df_expos["mgmBldrgstPk"] == pk] if pk is not None else df_expos

                    if "exposPubuseGbCdNm" in grp.columns:
                        df_j = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("전유", na=False)]
                        df_g = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("공용", na=False)]
                    else:
                        df_j, df_g = grp, pd.DataFrame()

                    if df_j.empty:
                        df_j = grp

                    j_area = sum(to_float(x) for x in df_j["area"].tolist()) if (not is_missing_area and "area" in df_j.columns) else 0.0
                    g_area = sum(to_float(x) for x in df_g["area"].tolist()) if (not is_missing_area and not df_g.empty and "area" in df_g.columns) else 0.0
                    t_area = j_area + g_area
                    mr = df_j.iloc[0]

                    full_nm = " ".join(filter(None, [
                        safe_val(mr.get("bldNm"), ""),
                        safe_val(mr.get("dongNm"), ""),
                        safe_val(mr.get("hoNm"), ""),
                    ]))

                    with st.container(border=True):
                        st.markdown(f"#### {full_nm}")
                        st.warning("🔒 소유자 정보는 개인정보 보호로 제공되지 않습니다.")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 주용도 | {safe_val(mr.get('mainPurpsCdNm'))} |
| 기타용도 | {safe_val(mr.get('etcPurps'))} |
| 구조 | {safe_val(mr.get('strctCdNm'))} |
| 해당 층 | {safe_val(mr.get('flrNoNm'))} |
                            """)
                        with c2:
                            j_disp = f"**{j_area:,.2f} ㎡**" if not is_missing_area else "⚠️ 누락"
                            g_disp = f"{g_area:,.2f} ㎡" if not is_missing_area else "⚠️ 누락"
                            t_disp = f"**{t_area:,.2f} ㎡**" if not is_missing_area else "확인 불가"
                            st.markdown(f"""
| 면적 구분 | 면적 |
|---|---|
| 전용면적 | {j_disp} |
| 공용면적 | {g_disp} |
| 계약면적(합계) | {t_disp} |
| 평형 환산 | 약 {t_area / 3.3058:.1f} 평 |
                            """)

        st.markdown("---")
        dong_label = (" — " + dong_in + "동") if dong_in else ""
        st.markdown(f"### 🪜 층별개요{dong_label}")
        if df_floor.empty:
            st.info("층별개요 데이터가 없습니다.")
        else:
            fl_col = {k: v for k, v in {
                "dongNm":"동명칭", "flrNoNm":"층", "mainPurpsCdNm":"주용도",
                "strctCdNm":"구조", "area":"면적(㎡)"
            }.items() if k in df_floor.columns}
            df_fl = df_floor[list(fl_col.keys())].rename(columns=fl_col).copy()
            df_fl.index = range(1, len(df_fl) + 1)
            st.caption(f"총 {len(df_fl)}개 층 데이터")
            st.dataframe(df_fl, use_container_width=True)

        st.markdown("---")
        st.caption("※ 국토교통부 건축HUB API 기반 / 법적 효력 없음 / 공식 증명서는 정부24·세움터에서 발급")

