# ============================================================
# 부동산 올인원 봇 — 실거래가 + 건축물대장 최종 완성본 v6
# 수정사항:
#   1. parse_platplc: "번지" 접미사 처리
#   2. fetch_expos_api: hoNm에서 "호" 제거 후 전달
#   3. target_exact_jibun: 단동 건물(동 미입력) 대응
#   4. API dongNm/hoNm 파라미터 직접 전달로 외필지 문제 해결
# ============================================================
import streamlit as st
import pandas as pd
import requests
import xmltodict
import re
import time
from io import BytesIO

# ─────────────────────────────────────────────────────────────
# 1. API 키
# ─────────────────────────────────────────────────────────────
DONG_API_KEY  = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

# ─────────────────────────────────────────────────────────────
# 2. 실거래가 API 경로
# ─────────────────────────────────────────────────────────────
API_PATHS = {
    "아파트_매매":        "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "아파트_전월세":       "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "아파트분양권_매매":   "RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade",
    "오피스텔_매매":       "RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "오피스텔_전월세":     "RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "연립/다세대_매매":    "RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "연립/다세대_전월세":  "RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "단독/다가구_매매":    "RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "단독/다가구_전월세":  "RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    "상업/업무용_매매":    "RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
    "공장 및 창고_매매":   "RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade",
    "토지_매매":           "RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade",
}

# ─────────────────────────────────────────────────────────────
# 3. 건축물대장 API URL
# ─────────────────────────────────────────────────────────────
_BASE     = "http://apis.data.go.kr/1613000/BldRgstHubService"
URL_RECAP = f"{_BASE}/getBrRecapTitleInfo"
URL_TITLE = f"{_BASE}/getBrTitleInfo"
URL_EXPOS = f"{_BASE}/getBrExposPubuseAreaInfo"
URL_FLOOR = f"{_BASE}/getBrFlrOulnInfo"
URL_ATCH  = f"{_BASE}/getBrAtchJibunInfo"

# ─────────────────────────────────────────────────────────────
# 4. 공통 헬퍼
# ─────────────────────────────────────────────────────────────
def safe_val(val, default="-"):
    if val is None: return default
    s = str(val).strip()
    return default if s in ("", "None", "nan") else s

def fmt_date(d):
    s = safe_val(d)
    return f"{s[:4]}.{s[4:6]}.{s[6:]}" if (len(s) == 8 and s.isdigit()) else s

def to_float(v):
    try: return float(str(v).replace(",", "").strip())
    except: return 0.0

def parse_address(addr):
    parts = addr.strip().split()
    if not parts: return None, None, None, None
    plat_gb = "0"
    if "산" in parts:
        plat_gb = "1"; parts.remove("산")
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
    """
    '서울특별시 동작구 상도동 363-164번지' → ('0','0363','0164')
    '서울특별시 동작구 상도동 450-7'       → ('0','0450','0007')
    '서울특별시 동작구 상도동 450'         → ('0','0450','0000')
    '서울특별시 동작구 상도동 산 12'       → ('1','0012','0000')
    """
    s = str(platplc).strip()
    if not s or s in ("None", "nan", "-"): return None, None, None
    # ★ "번지", "번" 등 한글 접미사 제거
    s = re.sub(r'[번지]+\s*$', '', s).strip()
    pgb = "1" if re.search(r'\s산\s*\d', s) else "0"
    m = re.search(r'(\d+)(?:-(\d+))?\s*$', s)
    if not m: return None, None, None
    bun = str(m.group(1)).zfill(4)
    ji  = str(m.group(2)).zfill(4) if m.group(2) else "0000"
    return pgb, bun, ji

def get_bjdong_code(search_term):
    url = (f"https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
           f"?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=10&type=json&locatadd_nm={search_term}")
    try:
        data = requests.get(url, timeout=10).json()
        if data.get("StanReginCd"):
            rows   = data["StanReginCd"][1]["row"]
            active = [r for r in rows if r["sido_cd"] != "" and r["sgg_cd"] != ""]
            if active:
                rc = active[0]["region_cd"]
                return rc[:5], rc[5:10], active[0]["locatadd_nm"]
    except: pass
    return None, None, None

def match_dong(target, dong_nm, bld_nm):
    if not target: return True
    dong_empty = not str(dong_nm).strip() or str(dong_nm).strip() in ("", "None", "nan")
    bld_empty  = not str(bld_nm).strip()  or str(bld_nm).strip()  in ("", "None", "nan")
    if dong_empty and bld_empty: return False
    t = re.sub(r"[^A-Za-z0-9가-힣]", "", str(target)).replace("동","").replace("제","").upper()
    d = re.sub(r"[^A-Za-z0-9가-힣]", "", str(dong_nm)).replace("동","").replace("제","").upper()
    b = re.sub(r"[^A-Za-z0-9가-힣]", "", str(bld_nm)).upper()
    if t and d and t == d: return True
    if t and f"{t}동" in b: return True
    nums = re.findall(r"\d+", t)
    if nums:
        n = nums[-1]; short = str(int(n) % 100)
        if not d: return False
        if d in (f"주{short}", short, f"주{n}", n): return True
        if f"{n}동" in b: return True
    return False

def match_ho(target, ho_nm):
    if not target: return True
    t = re.sub(r"[^A-Za-z0-9가-힣]", "", str(target)).replace("호","").replace("제","").upper()
    h = re.sub(r"[^A-Za-z0-9가-힣]", "", str(ho_nm)).replace("호","").replace("제","").upper()
    if t == h: return True
    tn = re.findall(r"\d+", t); hn = re.findall(r"\d+", h)
    return bool(tn and hn and tn[-1] == hn[-1])

# ─────────────────────────────────────────────────────────────
# 5. 기본 API 호출
# ─────────────────────────────────────────────────────────────
def fetch_bld_api(endpoint, sgg_cd, bjdong_cd, plat_gb, bun, ji, max_pages=50):
    all_items = []
    for page in range(1, max_pages + 1):
        url = (f"{endpoint}?serviceKey={MOLIT_API_KEY}"
               f"&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}"
               f"&platGbCd={plat_gb}&bun={bun}&ji={ji}"
               f"&numOfRows=1000&pageNo={page}")
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=15)
                if res.text.strip().startswith("<"):
                    xml_data = xmltodict.parse(res.content)
                    if "OpenAPI_ServiceResponse" not in xml_data: break
            except: time.sleep(0.5)
        body  = xml_data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", []) if body.get("items") else []
        if isinstance(items, dict): items = [items]
        if not items: break
        all_items.extend(items)
        if page * 1000 >= int(body.get("totalCount", 0)): break
    return all_items

# ─────────────────────────────────────────────────────────────
# ★ 전유공용면적 전용: dongNm + hoNm을 API 파라미터로 직접 전달
#   - hoNm: "호" 접미사 제거 후 전달 (API DB는 숫자만 저장)
#   - dongNm: "동" 접미사 유지 (API 문서 예시 기준)
# ─────────────────────────────────────────────────────────────
def fetch_expos_api(sgg_cd, bjdong_cd, plat_gb, bun, ji, dong_nm="", ho_nm="", max_pages=10):
    all_items = []
    for page in range(1, max_pages + 1):
        params = (f"?serviceKey={MOLIT_API_KEY}"
                  f"&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}"
                  f"&platGbCd={plat_gb}&bun={bun}&ji={ji}"
                  f"&numOfRows=1000&pageNo={page}")
        if dong_nm.strip():
            params += f"&dongNm={requests.utils.quote(dong_nm.strip(), safe='')}"
        if ho_nm.strip():
            # ★ "호" 제거: API DB 저장값은 숫자만 (예: "407", "306")
            clean_ho = re.sub(r'호$', '', ho_nm.strip())
            params += f"&hoNm={requests.utils.quote(clean_ho, safe='')}"
        url = URL_EXPOS + params
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=15)
                if res.text.strip().startswith("<"):
                    xml_data = xmltodict.parse(res.content)
                    if "OpenAPI_ServiceResponse" not in xml_data: break
            except: time.sleep(0.5)
        body  = xml_data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", []) if body.get("items") else []
        if isinstance(items, dict): items = [items]
        if not items: break
        all_items.extend(items)
        if page * 1000 >= int(body.get("totalCount", 0)): break
    return all_items

# ─────────────────────────────────────────────────────────────
# 6. 부속지번 수집
# ─────────────────────────────────────────────────────────────
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
        if c not in result: result.append(c)
    return result

# ─────────────────────────────────────────────────────────────
# 7. 건축물대장 메인 조회
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

    # STEP 0: 부속지번 수집
    status.info("🗺️ 단지 필지 구성 파악 중...")
    all_jibun = get_all_jibun(sgg_cd, bjdong_cd, plat_gb, bun, ji)

    # STEP 1: 표제부 수집 (전 필지)
    status.info("📋 표제부 수집 중...")
    raw_titles = []
    for (js, jb, jp, jbun, jji) in all_jibun:
        for p_gb in plat_cands:
            items = fetch_bld_api(URL_TITLE, js, jb, p_gb, jbun, jji, max_pages=10)
            valid = [x for x in items if to_float(x.get("totArea", "0")) > 0]
            if valid:
                raw_titles.extend(valid); break

    seen, unique_titles = set(), []
    for item in raw_titles:
        pk = item.get("mgmBldrgstPk")
        if pk not in seen:
            seen.add(pk); unique_titles.append(item)
    df_titles = pd.DataFrame(unique_titles) if unique_titles else pd.DataFrame()
    for item in unique_titles:
        pk = item.get("mgmBldrgstPk")
        if pk:
            pk_map[pk] = {"dong": item.get("dongNm", ""), "bld": item.get("bldNm", "")}

    # 동 필터 PK 세트
    target_pks = set()
    if target_dong and not df_titles.empty:
        for _, row in df_titles.iterrows():
            if match_dong(target_dong, row.get("dongNm", ""), row.get("bldNm", "")):
                pk = row.get("mgmBldrgstPk")
                if pk: target_pks.add(pk)

    # ★ 표제부 platPlc에서 실제 지번 추출
    # target_dong 있으면 해당 동만, 없으면 전체 (단동 건물 대응)
    target_exact_jibun = []
    if not df_titles.empty:
        seen_j = set()
        for _, row in df_titles.iterrows():
            if target_dong and not match_dong(target_dong, row.get("dongNm", ""), row.get("bldNm", "")):
                continue
            for plc_field in ["platPlc", "newPlatPlc"]:
                pgb, bn, jn = parse_platplc(row.get(plc_field, ""))
                if bn:
                    key = (sgg_cd, bjdong_cd, pgb, bn, jn)
                    if key not in seen_j:
                        seen_j.add(key); target_exact_jibun.append(key)
                    break

    # STEP 2: 총괄표제부
    status.info("🏢 총괄표제부 수집 중...")
    df_recap = pd.DataFrame()
    for p_gb in ["0", "2", "3", "1"]:
        items = fetch_bld_api(URL_RECAP, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=2)
        valid = [x for x in items if to_float(x.get("totArea", "0")) > 0]
        if valid:
            df_recap = pd.DataFrame(valid); break

    # ─────────────────────────────────────────────────────────
    # STEP 3: 전유공용면적
    # 탐색 순서:
    #   ① platPlc 파싱 지번 + dongNm + hoNm API 파라미터 직접 전달
    #   ② dongNm 없이 hoNm만으로 재시도 (동명 불일치 대비)
    #   ③ 부속지번 전체 탐색 (최후 fallback)
    # ─────────────────────────────────────────────────────────
    status.info("🏠 전유공용면적 수집 중...")
    df_expos        = pd.DataFrame()
    is_missing_area = False

    if target_ho:
        # API에 넘길 동명/호명 정리
        api_dong_nm = ""
        if target_dong:
            t = target_dong.strip()
            api_dong_nm = t if t.endswith("동") else t + "동"

        # ★ hoNm: "호" 제거 (API DB는 숫자만 저장)
        api_ho_nm = re.sub(r'호$', '', target_ho.strip())

        found = False
        # platPlc 지번 우선, 그 다음 부속지번
        search_jibun = target_exact_jibun + [j for j in all_jibun if j not in target_exact_jibun]

        # ① dongNm + hoNm 둘 다 전달
        for (js, jb, jp, jbun, jji) in search_jibun:
            if found: break
            for p_gb in ([jp] + [x for x in plat_cands if x != jp]):
                items = fetch_expos_api(
                    js, jb, p_gb, jbun, jji,
                    dong_nm=api_dong_nm,
                    ho_nm=api_ho_nm,
                    max_pages=10
                )
                if not items: continue
                df_expos = pd.DataFrame(items)
                if "area" not in df_expos.columns:
                    df_expos["area"] = "0"; is_missing_area = True
                found = True; break

        # ② dongNm 없이 hoNm만으로 재시도
        if not found:
            for (js, jb, jp, jbun, jji) in search_jibun:
                if found: break
                for p_gb in ([jp] + [x for x in plat_cands if x != jp]):
                    items = fetch_expos_api(
                        js, jb, p_gb, jbun, jji,
                        dong_nm="",
                        ho_nm=api_ho_nm,
                        max_pages=10
                    )
                    if not items: continue
                    tmp = pd.DataFrame(items)
                    tmp["dongNm"] = tmp.apply(lambda r: restore(r, "dongNm"), axis=1)
                    tmp["bldNm"]  = tmp.apply(lambda r: restore(r, "bldNm"),  axis=1)
                    if target_dong:
                        m_pk = tmp[tmp["mgmBldrgstPk"].isin(target_pks)] \
                               if ("mgmBldrgstPk" in tmp.columns and target_pks) else pd.DataFrame()
                        m_name = tmp[tmp.apply(
                            lambda r: match_dong(target_dong, r.get("dongNm",""), r.get("bldNm","")), axis=1
                        )]
                        if not m_pk.empty:     tmp = m_pk
                        elif not m_name.empty: tmp = m_name
                        else: continue
                    if tmp.empty: continue
                    df_expos = tmp.copy()
                    if "area" not in df_expos.columns:
                        df_expos["area"] = "0"; is_missing_area = True
                    found = True; break

    # STEP 4: 층별개요 (platPlc 지번 우선)
    status.info("🪜 층별개요 수집 중...")
    df_floor = pd.DataFrame()
    found    = False
    floor_order = target_exact_jibun + [j for j in all_jibun if j not in target_exact_jibun]

    for (js, jb, jp, jbun, jji) in floor_order:
        if found: break
        for p_gb in ([jp] + [x for x in plat_cands if x != jp]):
            items = fetch_bld_api(URL_FLOOR, js, jb, p_gb, jbun, jji, max_pages=20)
            if not items: continue
            tmp = pd.DataFrame(items)
            tmp["dongNm"] = tmp.apply(lambda r: restore(r, "dongNm"), axis=1)
            tmp["bldNm"]  = tmp.apply(lambda r: restore(r, "bldNm"),  axis=1)
            if target_pks:
                tmp = tmp[tmp["mgmBldrgstPk"].isin(target_pks)]
            elif target_dong:
                tmp = tmp[tmp.apply(
                    lambda r: match_dong(target_dong, r.get("dongNm",""), r.get("bldNm","")), axis=1
                )]
            if not tmp.empty:
                tmp = tmp.copy()
                tmp["_n"] = pd.to_numeric(
                    tmp.get("flrNo", pd.Series(dtype=str)), errors="coerce"
                ).fillna(-99)
                df_floor = tmp.sort_values("_n", ascending=False).drop(columns=["_n"])
                found = True; break

    status.empty()
    return df_recap, df_titles, df_expos, df_floor, is_missing_area, len(all_jibun)

# ─────────────────────────────────────────────────────────────
# 8. 실거래가 함수
# ─────────────────────────────────────────────────────────────
def get_sigungu_code(sigungu_name, dong_name):
    search_term = dong_name.strip() if dong_name.strip() else sigungu_name.strip()
    url = (f"https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
           f"?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=500&type=json&locatadd_nm={search_term}")
    try:
        res = requests.get(url, timeout=10)
        if not res.text.strip(): return None, None
        data = res.json()
        if data.get("StanReginCd"):
            rows   = data["StanReginCd"][1]["row"]
            active = [r for r in rows if r["sido_cd"] != "" and r["sgg_cd"] != ""]
            for region in active:
                if sigungu_name.strip() in region["locatadd_nm"]:
                    return region["region_cd"][:5], region["locatadd_nm"]
        return None, None
    except: return None, None

def get_real_estate_data(sigungu_code, start_month, end_month, dong_name, prop_type, trans_type):
    dict_key = f"{prop_type}_{trans_type}"
    if dict_key not in API_PATHS:
        st.warning(f"⚠️ '{prop_type} {trans_type}' 조합은 지원하지 않습니다.")
        return pd.DataFrame()
    base_url = f"http://apis.data.go.kr/1613000/{API_PATHS[dict_key]}"
    try:
        month_list = pd.date_range(
            pd.to_datetime(start_month, format="%Y%m"),
            pd.to_datetime(end_month,   format="%Y%m"), freq="MS"
        ).strftime("%Y%m").tolist()
    except:
        st.error("조회 기간 형식 오류 (YYYYMM)"); return pd.DataFrame()

    all_data = []; progress_bar = st.progress(0); status_text = st.empty()
    for i, ym in enumerate(month_list):
        status_text.text(f"⏳ {ym} 조회 중... ({i+1}/{len(month_list)})")
        progress_bar.progress((i+1) / len(month_list))
        url = (f"{base_url}?serviceKey={MOLIT_API_KEY}"
               f"&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={ym}")
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if not res.text.strip().startswith("<"): break
            xml_data = xmltodict.parse(res.content)
            if "OpenAPI_ServiceResponse" in xml_data: break
            if xml_data.get("response",{}).get("header",{}).get("resultCode") not in ["00","0","200","000"]: continue
            items = xml_data.get("response",{}).get("body",{}).get("items")
            if items and "item" in items:
                il = items["item"]
                if isinstance(il, dict): il = [il]
                all_data.append(pd.DataFrame(il))
        except: continue
        time.sleep(0.3)
    status_text.empty(); progress_bar.empty()
    if not all_data:
        st.warning("거래 내역이 없거나 조회가 중단되었습니다."); return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    if dong_name.strip():
        df = df[df["umdNm"].str.contains(dong_name.strip(), na=False)]
    if df.empty:
        st.warning(f"'{dong_name}' 거래 내역 없음"); return pd.DataFrame()

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
    if all(x in df.columns for x in ["년","월","일"]):
        df["계약일"] = (df["년"].astype(str) + "-"
                       + df["월"].astype(str).str.zfill(2) + "-"
                       + df["일"].astype(str).str.zfill(2))
    if trans_type == "매매" and "거래금액" in df.columns:
        area_col = next((c for c in ["전용면적","건물면적","연면적","거래면적","대지면적","계약면적"] if c in df.columns), None)
        if area_col:
            def pyeong(row):
                try:
                    p = int(str(row["거래금액"]).replace(",", ""))
                    a = float(str(row[area_col]).replace(",", ""))
                    if a <= 0: return ""
                    pp = int(p / (a / 3.3058)); uk, man = pp // 10000, pp % 10000
                    return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{pp}만원"
                except: return ""
            df["평당가격"] = df.apply(pyeong, axis=1)
    disp = ["계약일","소재지","건물유형","용도지역","건물주용도","건물명","건축년도",
            "대지면적","건물면적","연면적","전용면적","층","거래금액","평당가격",
            "매수자","매도자","지분거래여부","거래유형","중개사소재지","계약취소일"]
    df = df[[c for c in disp if c in df.columns]].copy()
    def fmt_money(v):
        if pd.isna(v): return ""
        try:
            p = int(str(v).replace(",", "")); uk, man = p // 10000, p % 10000
            return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{p}만원"
        except: return v
    for col in ["거래금액","보증금"]:
        if col in df.columns: df[col] = df[col].apply(fmt_money)
    if "계약일" in df.columns: df = df.sort_values("계약일", ascending=False)
    return df

# ─────────────────────────────────────────────────────────────
# 9. Streamlit UI
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 건축물대장 조회"])

# ════ 탭 1: 실거래가 ════════════════════════════════════════
with tab1:
    now = pd.Timestamp.now()
    with st.form("form_trade"):
        c1, c2 = st.columns(2)
        with c1: prop_t = st.selectbox("매물 종류", ["아파트","아파트분양권","오피스텔","연립/다세대","단독/다가구","상업/업무용","공장 및 창고","토지"])
        with c2: tran_t = st.selectbox("거래 종류", ["매매","전월세"])
        c3, c4, c5, c6 = st.columns(4)
        with c3: sgg_nm = st.text_input("시/군/구", value="서초구")
        with c4: dn_nm  = st.text_input("법정동 (빈칸=구 전체)")
        with c5: s_mon  = st.text_input("시작월 YYYYMM", value=(now - pd.DateOffset(months=1)).strftime("%Y%m"))
        with c6: e_mon  = st.text_input("종료월 YYYYMM", value=now.strftime("%Y%m"))
        sub1 = st.form_submit_button("🔍 실거래가 조회")
    if sub1:
        if not sgg_nm: st.warning("시/군/구를 입력하세요.")
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
            else: st.error("지역을 찾을 수 없습니다.")

# ════ 탭 2: 건축물대장 ══════════════════════════════════════
with tab2:
    st.subheader("📋 건축물대장 종합 조회")
    st.caption("💡 아파트·오피스텔·도시형생활주택 등 집합건물 전유공용면적 조회 지원 (외필지 자동 처리)")

    with st.form("form_bld"):
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1: addr_in = st.text_input("지번 주소 (필수)", placeholder="예: 상도동 450  /  상도동 363-164")
        with c2: dong_in = st.text_input("동 (선택)", placeholder="예: 105  (단동이면 비워도 됨)")
        with c3: ho_in   = st.text_input("호수 (선택)", placeholder="예: 302")
        sub2 = st.form_submit_button("🔍 건축물대장 열람")

    if sub2 and addr_in:
        region_term, plat_gb, bun, ji = parse_address(addr_in)
        if not region_term:
            st.warning("주소 형식을 확인해주세요."); st.stop()
        sgg_cd, bjdong_cd, full_loc = get_bjdong_code(region_term)
        if not sgg_cd:
            st.error("지역 코드를 찾을 수 없습니다."); st.stop()

        st.success(f"✅ {full_loc}  본번:{bun}  부번:{ji}")

        df_recap, df_titles, df_expos, df_floor, is_missing_area, jibun_cnt = get_building_ledger(
            sgg_cd, bjdong_cd, plat_gb, bun, ji, dong_in, ho_in
        )
        st.caption(f"📌 탐색 필지: {jibun_cnt}개 | 전유부: platPlc 지번 파싱 + API dongNm/hoNm 직접 전달")

        if df_recap.empty and df_titles.empty:
            st.error("🚨 조회 결과 없음. 지번 주소를 다시 확인해주세요."); st.stop()

        viol_src = pd.concat([df for df in [df_recap, df_titles] if not df.empty], ignore_index=True)
        if "violBldYn" in viol_src.columns:
            if viol_src["violBldYn"].astype(str).str.contains("1").any():
                st.error("🚨 [주의] 위반건축물 대장입니다! 정부24 원본 서류를 반드시 확인하세요.")

        # ── 섹션 A: 총괄표제부 ──────────────────────────────
        if not df_recap.empty:
            r = df_recap.iloc[0]
            st.markdown("---")
            st.markdown("### 🏢 총괄표제부")
            st.markdown(f"**📍 {safe_val(r.get('bldNm'), '명칭없음')}**  `{safe_val(r.get('platPlc', r.get('newPlatPlc')))}`")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 주용도 | {safe_val(r.get('mainPurpsCdNm'))} |
| 대지면적 | {safe_val(r.get('platArea'))} ㎡ |
| 건축면적 | {safe_val(r.get('archArea'))} ㎡ |
| 연면적(총) | {safe_val(r.get('totArea'))} ㎡ |
| 건폐율 | {safe_val(r.get('bcRat'))} % |
| 용적률 | {safe_val(r.get('vlRat'))} % |
                """)
            with c2:
                st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 총 세대수 | {safe_val(r.get('hhldCnt'), '0')} 세대 |
| 총 가구수 | {safe_val(r.get('fmlyCnt'), '0')} 가구 |
| 총 주차대수 | {safe_val(r.get('totPkngCnt'), '0')} 대 |
| 주건축물수 | {safe_val(r.get('mainBldCnt'), '0')} 동 |
| 부속건축물수 | {safe_val(r.get('atchBldCnt'), '0')} 동 |
| 에너지효율등급 | {safe_val(r.get('engrGrade'))} |
                """)

        # ── 섹션 B: 표제부 ──────────────────────────────────
        if not df_titles.empty:
            st.markdown("---")
            st.markdown("### 📄 표제부 — 전체 동 목록")
            sum_cols = {
                "bldNm":"건물명", "dongNm":"동명칭", "platPlc":"대지위치",
                "mainPurpsCdNm":"주용도", "strctCdNm":"주구조",
                "grndFlrCnt":"지상층", "ugrndFlrCnt":"지하층",
                "heit":"높이(m)", "totArea":"연면적(㎡)", "hhldCnt":"세대수",
                "useAprDay":"사용승인일", "violBldYn":"위반건축물",
            }
            ex = {k: v for k, v in sum_cols.items() if k in df_titles.columns}
            df_show = df_titles[list(ex.keys())].rename(columns=ex).copy()
            if "사용승인일" in df_show.columns:
                df_show["사용승인일"] = df_show["사용승인일"].apply(fmt_date)
            if "위반건축물" in df_show.columns:
                df_show["위반건축물"] = df_show["위반건축물"].apply(
                    lambda x: "⚠️ 위반" if str(x).strip() == "1" else "정상"
                )
            df_show.index = range(1, len(df_show) + 1)
            st.caption(f"총 {len(df_show)}개 동 조회됨")
            st.dataframe(df_show, use_container_width=True)

            if dong_in:
                filtered = df_titles[df_titles.apply(
                    lambda r: match_dong(dong_in, r.get("dongNm",""), r.get("bldNm","")), axis=1
                )]
                dr    = filtered.iloc[0] if not filtered.empty else df_titles.iloc[0]
                label = dong_in + "동"
            else:
                dr    = df_titles.iloc[0]
                label = safe_val(dr.get("dongNm"), safe_val(dr.get("bldNm"), "건물"))

            actual_loc = safe_val(dr.get("platPlc",""), "")
            if actual_loc and actual_loc != "-":
                st.info(f"📍 [{label}] 실제 지번: **{actual_loc}**")

            with st.expander(f"📌 [{label}] 동 상세 정보", expanded=bool(dong_in)):
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 건물명 | {safe_val(dr.get('bldNm'))} |
| 동명칭 | {safe_val(dr.get('dongNm'))} |
| 대지위치 | {safe_val(dr.get('platPlc'))} |
| 도로명주소 | {safe_val(dr.get('newPlatPlc'))} |
| 주용도 | {safe_val(dr.get('mainPurpsCdNm'))} |
| 기타용도 | {safe_val(dr.get('etcPurps'))} |
| 대장구분 | {safe_val(dr.get('regstrGbCdNm'))} |
| 대장종류 | {safe_val(dr.get('regstrKindCdNm'))} |
                    """)
                with c2:
                    st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 주구조 | {safe_val(dr.get('strctCdNm'))} |
| 지붕 | {safe_val(dr.get('roofCdNm'))} |
| 규모 | 지하 {safe_val(dr.get('ugrndFlrCnt'),'0')}층 / 지상 {safe_val(dr.get('grndFlrCnt'),'0')}층 |
| 높이 | {safe_val(dr.get('heit'))} m |
| 대지면적 | {safe_val(dr.get('platArea'))} ㎡ |
| 건축면적 | {safe_val(dr.get('archArea'))} ㎡ |
| 연면적 | {safe_val(dr.get('totArea'))} ㎡ |
| 용적률산정연면적 | {safe_val(dr.get('vlRatEstmTotArea'))} ㎡ |
                    """)
                c3, c4 = st.columns(2)
                with c3:
                    st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 건폐율 | {safe_val(dr.get('bcRat'))} % |
| 용적률 | {safe_val(dr.get('vlRat'))} % |
| 세대수 | {safe_val(dr.get('hhldCnt'),'0')} 세대 |
| 가구수 | {safe_val(dr.get('fmlyCnt'),'0')} 가구 |
| 승용승강기 | {safe_val(dr.get('rideUseElvtCnt'),'0')} 대 |
| 비상승강기 | {safe_val(dr.get('emgenUseElvtCnt'),'0')} 대 |
                    """)
                with c4:
                    st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 허가일 | {fmt_date(dr.get('pmsDay'))} |
| 착공일 | {fmt_date(dr.get('stcDay'))} |
| 사용승인일 | {fmt_date(dr.get('useAprDay'))} |
| 내진설계 적용 | {safe_val(dr.get('rsthqAbltyYn'))} |
| 내진능력 | {safe_val(dr.get('rsthqAblty'))} |
| 에너지효율등급 | {safe_val(dr.get('engrGrade'))} |
| 친환경건축물등급 | {safe_val(dr.get('gnBldGrade'))} |
| 지능형건축물등급 | {safe_val(dr.get('itgBldGrade'))} |
                    """)

        # ── 섹션 C: 전유공용면적 ────────────────────────────
        if ho_in:
            st.markdown("---")
            dong_label = (dong_in + "동 ") if dong_in else ""
            st.markdown(f"### 🚪 전유공용면적 — {dong_label}{ho_in}호")
            if df_expos.empty:
                st.warning(
                    "⚠️ 전유공용면적 데이터를 찾을 수 없습니다.\n\n"
                    "**확인 사항:**\n"
                    "- 집합건물(아파트·오피스텔·도시형생활주택·다세대)만 존재합니다\n"
                    "- 호수 형식 확인 (예: 306호 → `306`)\n"
                    "- 단동 건물은 동 칸을 비워두세요"
                )
            else:
                pks = df_expos["mgmBldrgstPk"].unique() if "mgmBldrgstPk" in df_expos.columns else [None]
                for pk in pks[:3]:
                    grp = df_expos[df_expos["mgmBldrgstPk"] == pk] if pk else df_expos
                    if "exposPubuseGbCdNm" in grp.columns:
                        df_j = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("전유", na=False)]
                        df_g = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("공용", na=False)]
                    else:
                        df_j, df_g = grp, pd.DataFrame()
                    if df_j.empty: df_j = grp
                    j_area = sum(to_float(x) for x in df_j["area"].tolist()) if (not is_missing_area and "area" in df_j.columns) else 0.0
                    g_area = sum(to_float(x) for x in df_g["area"].tolist()) if (not is_missing_area and not df_g.empty and "area" in df_g.columns) else 0.0
                    t_area = j_area + g_area
                    mr     = df_j.iloc[0]
                    d_disp = safe_val(mr.get("dongNm"), (dong_in + "동") if dong_in else "")
                    full_nm = " ".join(filter(None, [
                        safe_val(mr.get("bldNm"), ""), d_disp,
                        safe_val(mr.get("hoNm"), ho_in + "호")
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
                            j_disp = f"**{j_area:,.2f} ㎡**" if not is_missing_area else "⚠️ API 누락"
                            g_disp = f"{g_area:,.2f} ㎡"      if not is_missing_area else "⚠️ 누락"
                            t_disp = f"**{t_area:,.2f} ㎡**"  if not is_missing_area else "확인 불가"
                            st.markdown(f"""
| 면적 구분 | 면적 |
|---|---|
| 전용면적 | {j_disp} |
| 공용면적 | {g_disp} |
| **계약면적(합계)** | {t_disp} |
| 평형 환산 | 약 {t_area / 3.3058:.1f} 평 |
                            """)
                        if "exposPubuseGbCdNm" in grp.columns:
                            with st.expander("면적 상세 내역 펼치기"):
                                dc = grp[["exposPubuseGbCdNm","mainPurpsCdNm","area"]].copy()
                                dc.columns = ["전유/공용","용도","면적(㎡)"]
                                st.dataframe(dc, use_container_width=True)

        # ── 섹션 D: 층별개요 ────────────────────────────────
        st.markdown("---")
        dong_label = ("  —  " + dong_in + "동") if dong_in else ""
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





