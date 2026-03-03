import streamlit as st
import pandas as pd
import requests
import xmltodict
import time
import re
from io import BytesIO

# ─────────────────────────────────────────
# 1. API 키 설정
# ─────────────────────────────────────────
DONG_API_KEY  = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

# ─────────────────────────────────────────
# 2. 실거래가 API 경로 (기존 유지)
# ─────────────────────────────────────────
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

# ─────────────────────────────────────────
# [기능 1] 실거래가 (기존 코드 그대로)
# ─────────────────────────────────────────
def get_sigungu_code(sigungu_name, dong_name):
    base_url = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    search_term = dong_name.strip() if dong_name.strip() else sigungu_name.strip()
    url = f"{base_url}?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=500&type=json&locatadd_nm={search_term}"
    try:
        response = requests.get(url)
        if not response.text.strip(): return None, None
        data = response.json()
        if data.get("StanReginCd"):
            rows = data["StanReginCd"][1]["row"]
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
        progress_bar.progress((i+1)/len(month_list))
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={ym}"
        try:
            res = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
            if not res.text.strip().startswith("<"): break
            xml_data = xmltodict.parse(res.content)
            if "OpenAPI_ServiceResponse" in xml_data: break
            if xml_data.get("response",{}).get("header",{}).get("resultCode") not in ["00","0","200","000"]: continue
            items = xml_data.get("response",{}).get("body",{}).get("items")
            if items and "item" in items:
                item_list = items["item"]
                if isinstance(item_list, dict): item_list = [item_list]
                all_data.append(pd.DataFrame(item_list))
        except:
            continue
        time.sleep(0.3)
    status_text.empty(); progress_bar.empty()
    if not all_data:
        st.warning("거래 내역이 없거나 조회가 중단되었습니다.")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    if dong_name.strip():
        df = df[df["umdNm"].str.contains(dong_name.strip(), na=False)]
    if df.empty:
        st.warning(f"'{dong_name}' 지역 거래 내역 없음")
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
    if all(x in df.columns for x in ["년","월","일"]):
        df["계약일"] = df["년"].astype(str) + "-" + df["월"].astype(str).str.zfill(2) + "-" + df["일"].astype(str).str.zfill(2)
    if trans_type == "매매" and "거래금액" in df.columns:
        area_cols = ["전용면적","건물면적","연면적","거래면적","대지면적","계약면적"]
        area_col = next((c for c in area_cols if c in df.columns), None)
        if area_col:
            def pyeong(row):
                try:
                    p = int(str(row["거래금액"]).replace(",",""))
                    a = float(str(row[area_col]).replace(",",""))
                    if a <= 0: return ""
                    pp = int(p/(a/3.3058)); uk,man = pp//10000,pp%10000
                    return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{pp}만원"
                except: return ""
            df["평당가격"] = df.apply(pyeong, axis=1)
    disp = ["계약일","소재지","건물유형","용도지역","건물주용도","건물명","건축년도","대지면적","건물면적","연면적","전용면적","층","거래금액","평당가격","매수자","매도자","지분거래여부","거래유형","중개사소재지","계약취소일"]
    df = df[[c for c in disp if c in df.columns]].copy()
    def fmt_money(v):
        if pd.isna(v): return ""
        try:
            p = int(str(v).replace(",","").strip()); uk,man = p//10000,p%10000
            return (f"{uk}억 {man}만원" if man else f"{uk}억원") if uk else f"{p}만원"
        except: return v
    for col in ["거래금액","보증금"]:
        if col in df.columns: df[col] = df[col].apply(fmt_money)
    if "계약일" in df.columns: df = df.sort_values("계약일", ascending=False)
    return df


# ═══════════════════════════════════════════════════════════
# [기능 2] 건축물대장 — 완전 재작성 버전
# ═══════════════════════════════════════════════════════════

# ── 올바른 엔드포인트 상수 정의 ─────────────────────────────
_BASE = "http://apis.data.go.kr/1613000/BldRgstHubService"
URL_RECAP  = f"{_BASE}/getBrRecapTitleInfo"        # 총괄표제부
URL_TITLE  = f"{_BASE}/getBrTitleInfo"             # 표제부
URL_EXPOS  = f"{_BASE}/getBrExposPubuseAreaInfo"   # ✅ 전유공용면적 (핵심 수정)
URL_FLOOR  = f"{_BASE}/getBrFlrOulnInfo"           # 층별개요
URL_ZONE   = f"{_BASE}/getBrJijiguInfo"            # 지역지구구역


def safe_val(val, default="-"):
    if val is None: return default
    s = str(val).strip()
    return default if s in ("", "None", "nan") else s


def fmt_date(d):
    """YYYYMMDD → YYYY.MM.DD"""
    s = safe_val(d)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}.{s[4:6]}.{s[6:]}"
    return s


def parse_address_for_bldrgst(address_str):
    parts = address_str.strip().split()
    if not parts: return None, None, None, None
    plat_gb_cd = "0"
    if "산" in parts:
        plat_gb_cd = "1"
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
    return " ".join(parts), plat_gb_cd, bun, ji


def get_full_bjdong_code(search_term):
    url = (f"https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
           f"?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=10&type=json&locatadd_nm={search_term}")
    try:
        data = requests.get(url).json()
        if data.get("StanReginCd"):
            rows = data["StanReginCd"][1]["row"]
            active = [r for r in rows if r["sido_cd"] != "" and r["sgg_cd"] != ""]
            if active:
                rc = active[0]["region_cd"]
                return rc[:5], rc[5:10], active[0]["locatadd_nm"]
    except: pass
    return None, None, None


def fetch_api(endpoint, sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, max_pages=50):
    """공통 API 호출 함수 — 전 페이지 수집"""
    all_items = []
    for page in range(1, max_pages + 1):
        url = (f"{endpoint}?serviceKey={MOLIT_API_KEY}"
               f"&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}"
               f"&platGbCd={plat_gb_cd}&bun={bun}&ji={ji}"
               f"&numOfRows=1000&pageNo={page}")
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=15)
                if res.text.strip().startswith("<"):
                    xml_data = xmltodict.parse(res.content)
                    if "OpenAPI_ServiceResponse" not in xml_data:
                        break
            except: time.sleep(1)
        body  = xml_data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", []) if body.get("items") else []
        if isinstance(items, dict): items = [items]
        if not items: break
        all_items.extend(items)
        if page * 1000 >= int(body.get("totalCount", 0)): break
    return all_items


def match_dong(target, dong_nm, bld_nm):
    if not target: return True
    t = re.sub(r'[^A-Za-z0-9가-힣]', '', str(target)).replace("동","").replace("제","").upper()
    d = re.sub(r'[^A-Za-z0-9가-힣]', '', str(dong_nm)).replace("동","").replace("제","").upper()
    b = re.sub(r'[^A-Za-z0-9가-힣]', '', str(bld_nm)).upper()
    if t == d or f"{t}동" in b: return True
    nums = re.findall(r'\d+', t)
    if nums:
        n = nums[-1]; short = str(int(n) % 100)
        if d in (f"주{short}", short, f"주{n}", n): return True
        if f"{n}동" in b: return True
    return False


def match_ho(target, ho_nm):
    if not target: return True
    t = re.sub(r'[^A-Za-z0-9가-힣]', '', str(target)).replace("호","").replace("제","").upper()
    h = re.sub(r'[^A-Za-z0-9가-힣]', '', str(ho_nm)).replace("호","").replace("제","").upper()
    if t == h: return True
    tn = re.findall(r'\d+', t); hn = re.findall(r'\d+', h)
    return bool(tn and hn and tn[-1] == hn[-1])


# ── 메인 대장 조회 함수 ──────────────────────────────────────
def get_building_ledger(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji,
                        target_dong="", target_ho=""):
    """
    반환: (df_recap, df_titles, df_expos, df_floor, pk_map)
    df_titles: 전체 표제부 DataFrame (동 필터 없음)
    """
    status = st.empty()
    plat_cands = ["3","2","0"] if plat_gb_cd != "1" else ["1"]

    # ── PK 맵 공유 딕셔너리
    pk_map = {}

    def restore_dong(r):
        v = r.get("dongNm","")
        if not v or str(v).strip() in ("","None","nan"):
            pk = r.get("mgmBldrgstPk")
            if pk and pk in pk_map: return pk_map[pk].get("dong","")
        return v

    def restore_bld(r):
        v = r.get("bldNm","")
        if not v or str(v).strip() in ("","None","nan"):
            pk = r.get("mgmBldrgstPk")
            if pk and pk in pk_map: return pk_map[pk].get("bld","")
        return v

    # ── STEP 1: 표제부 전수 수집 ──────────────────────────
    status.info("📋 표제부 수집 중...")
    all_title_items = []
    found_plat = plat_gb_cd
    for p_gb in plat_cands:
        items = fetch_api(URL_TITLE, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=20)
        valid = [x for x in items if _to_float(x.get("totArea","0")) > 0]
        if valid:
            all_title_items = valid
            found_plat = p_gb
            break

    df_titles = pd.DataFrame(all_title_items) if all_title_items else pd.DataFrame()

    # pk_map 구성
    for item in all_title_items:
        pk = item.get("mgmBldrgstPk")
        if pk:
            pk_map[pk] = {"dong": item.get("dongNm",""), "bld": item.get("bldNm","")}

    # 동 필터용 PK 세트
    target_pks = set()
    if target_dong and not df_titles.empty:
        for _, row in df_titles.iterrows():
            if match_dong(target_dong, row.get("dongNm",""), row.get("bldNm","")):
                pk = row.get("mgmBldrgstPk")
                if pk: target_pks.add(pk)

    # ── STEP 2: 총괄표제부 ──────────────────────────────────
    status.info("🏢 총괄표제부 수집 중...")
    df_recap = pd.DataFrame()
    for p_gb in ["0","2","3","1"]:
        items = fetch_api(URL_RECAP, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=2)
        valid = [x for x in items if _to_float(x.get("totArea","0")) > 0]
        if valid:
            df_recap = pd.DataFrame(valid)
            break

    # ── STEP 3: 전유공용면적 (호수 입력 시) ────────────────
    status.info("🏠 전유공용면적 수집 중...")
    df_expos = pd.DataFrame()
    if target_ho:
        for p_gb in plat_cands:
            items = fetch_api(URL_EXPOS, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=50)
            if not items: continue
            tmp = pd.DataFrame(items)
            # dongNm / bldNm 복원
            tmp["dongNm"] = tmp.apply(restore_dong, axis=1)
            tmp["bldNm"]  = tmp.apply(restore_bld,  axis=1)
            # 호수 매칭
            matched = tmp[tmp.apply(lambda r: match_ho(target_ho, r.get("hoNm","")), axis=1)]
            # 동 매칭 (동 입력 시 추가 필터)
            if target_dong and not matched.empty:
                matched = matched[matched.apply(
                    lambda r: r.get("mgmBldrgstPk") in target_pks
                              or match_dong(target_dong, r.get("dongNm",""), r.get("bldNm","")),
                    axis=1
                )]
            if not matched.empty:
                df_expos = matched.copy()
                break

    # ── STEP 4: 층별개요 ────────────────────────────────────
    status.info("🪜 층별개요 수집 중...")
    df_floor = pd.DataFrame()
    for p_gb in plat_cands:
        items = fetch_api(URL_FLOOR, sgg_cd, bjdong_cd, p_gb, bun, ji, max_pages=20)
        if not items: continue
        tmp = pd.DataFrame(items)
        tmp["dongNm"] = tmp.apply(restore_dong, axis=1)
        tmp["bldNm"]  = tmp.apply(restore_bld,  axis=1)
        # 동 입력 시 해당 동만, 없으면 전체
        if target_pks:
            tmp = tmp[tmp["mgmBldrgstPk"].isin(target_pks)]
        elif target_dong:
            tmp = tmp[tmp.apply(lambda r: match_dong(target_dong, r.get("dongNm",""), r.get("bldNm","")), axis=1)]
        if not tmp.empty:
            # 층 번호 기준 정렬 (지하→지상)
            tmp["_flr_num"] = pd.to_numeric(tmp.get("flrNo", pd.Series()), errors="coerce").fillna(-99)
            df_floor = tmp.sort_values("_flr_num", ascending=False).drop(columns=["_flr_num"])
            break

    status.empty()
    return df_recap, df_titles, df_expos, df_floor, pk_map


def _to_float(v):
    try: return float(str(v).replace(",",""))
    except: return 0.0


# ═══════════════════════════════════════════════════════════
# [UI] Streamlit
# ═══════════════════════════════════════════════════════════
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 건축물대장 조회"])


# ─── 탭 1: 실거래가 ──────────────────────────────────────────
with tab1:
    now = pd.Timestamp.now()
    with st.form("form_trade"):
        c1, c2 = st.columns(2)
        with c1: prop_t = st.selectbox("매물 종류", ["아파트","아파트분양권","오피스텔","연립/다세대","단독/다가구","상업/업무용","공장 및 창고","토지"])
        with c2: tran_t = st.selectbox("거래 종류", ["매매","전월세"])
        c3,c4,c5,c6 = st.columns(4)
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
                    rdf.index = range(1, len(rdf)+1)
                    st.dataframe(rdf, use_container_width=True)
                    buf = BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter") as wr:
                        rdf.to_excel(wr, index=True, index_label="순번", sheet_name="실거래가")
                    st.download_button("📥 엑셀 다운로드", buf.getvalue(), "실거래가.xlsx")
            else:
                st.error("지역을 찾을 수 없습니다.")


# ─── 탭 2: 건축물대장 ─────────────────────────────────────────
with tab2:
    st.subheader("📋 건축물대장 종합 조회")
    st.caption("총괄표제부 · 표제부(동 목록) · 전유공용면적(호수) · 층별개요를 한 번에 조회합니다.")

    with st.form("form_bld"):
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1: addr_in = st.text_input("지번 주소 (필수)", placeholder="예: 사당동 300  /  상도동 450-3")
        with c2: dong_in = st.text_input("동 (선택)",   placeholder="예: 101")
        with c3: ho_in   = st.text_input("호수 (선택)", placeholder="예: 502")
        sub2 = st.form_submit_button("🔍 건축물대장 열람")

    if sub2 and addr_in:
        region_term, plat_gb, bun, ji = parse_address_for_bldrgst(addr_in)
        if not region_term:
            st.warning("주소 형식을 확인해주세요.")
        else:
            sgg_cd, bjdong_cd, full_loc = get_full_bjdong_code(region_term)
            if not sgg_cd:
                st.error("지역 코드를 찾을 수 없습니다.")
            else:
                st.success(f"✅ {full_loc}  본번:{bun}  부번:{ji}  (시군구:{sgg_cd} / 법정동:{bjdong_cd})")

                df_recap, df_titles, df_expos, df_floor, pk_map = get_building_ledger(
                    sgg_cd, bjdong_cd, plat_gb, bun, ji, dong_in, ho_in
                )

                if df_recap.empty and df_titles.empty:
                    st.error("🚨 조회 결과 없음. 지번 주소를 다시 확인해주세요.")
                    st.stop()

                # ── 위반건축물 경고 ──────────────────────────
                viol_check = pd.concat([df for df in [df_recap, df_titles] if not df.empty], ignore_index=True)
                if "violBldYn" in viol_check.columns:
                    if viol_check["violBldYn"].astype(str).str.contains("1").any():
                        st.error("🚨 [주의] 위반건축물로 등록된 대장입니다! 정부24 원본 서류를 반드시 확인하세요.")

                # ════════════════════════════════════════════
                # 섹션 A: 총괄표제부
                # ════════════════════════════════════════════
                if not df_recap.empty:
                    r = df_recap.iloc[0]
                    st.markdown("---")
                    st.markdown("### 🏢 총괄표제부")
                    bld_nm = safe_val(r.get("bldNm"), "건물명 없음")
                    plc    = safe_val(r.get("platPlc"), safe_val(r.get("newPlatPlc")))
                    st.markdown(f"**📍 {bld_nm}** &nbsp;&nbsp; `{plc}`")

                    col1, col2 = st.columns(2)
                    with col1:
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
                    with col2:
                        st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 총 세대수 | {safe_val(r.get('hhldCnt'),'0')} 세대 |
| 총 가구수 | {safe_val(r.get('fmlyCnt'),'0')} 가구 |
| 총 주차대수 | {safe_val(r.get('totPkngCnt'),'0')} 대 |
| 주건축물수 | {safe_val(r.get('mainBldCnt'),'0')} 동 |
| 부속건축물수 | {safe_val(r.get('atchBldCnt'),'0')} 동 |
| 에너지효율등급 | {safe_val(r.get('engrGrade'))} |
                        """)

                # ════════════════════════════════════════════
                # 섹션 B: 표제부 — 전체 동 목록 + 상세
                # ════════════════════════════════════════════
                if not df_titles.empty:
                    st.markdown("---")
                    st.markdown("### 📄 표제부")

                    # ── 전체 동 요약 테이블 ─────────────────
                    summary_cols = {
                        "bldNm":          "건물명",
                        "dongNm":         "동명칭",
                        "mainPurpsCdNm":  "주용도",
                        "strctCdNm":      "주구조",
                        "grndFlrCnt":     "지상층",
                        "ugrndFlrCnt":    "지하층",
                        "heit":           "높이(m)",
                        "platArea":       "대지면적(㎡)",
                        "archArea":       "건축면적(㎡)",
                        "totArea":        "연면적(㎡)",
                        "hhldCnt":        "세대수",
                        "useAprDay":      "사용승인일",
                        "violBldYn":      "위반건축물",
                    }
                    exist_cols = {k:v for k,v in summary_cols.items() if k in df_titles.columns}
                    df_show = df_titles[list(exist_cols.keys())].rename(columns=exist_cols).copy()
                    if "사용승인일" in df_show.columns:
                        df_show["사용승인일"] = df_show["사용승인일"].apply(fmt_date)
                    if "위반건축물" in df_show.columns:
                        df_show["위반건축물"] = df_show["위반건축물"].apply(lambda x: "⚠️ 위반" if str(x).strip()=="1" else "정상")

                    st.caption(f"총 {len(df_show)}개 동/건물 조회됨")
                    st.dataframe(df_show, use_container_width=True)

                    # ── 선택된 동 (또는 첫 번째) 상세 정보 ──
                    if dong_in:
                        filtered = df_titles[df_titles.apply(
                            lambda r: match_dong(dong_in, r.get("dongNm",""), r.get("bldNm","")), axis=1
                        )]
                        detail_row = filtered.iloc[0] if not filtered.empty else df_titles.iloc[0]
                        detail_label = dong_in
                    else:
                        detail_row = df_titles.iloc[0]
                        detail_label = safe_val(detail_row.get("dongNm"), safe_val(detail_row.get("bldNm"), "1동"))

                    with st.expander(f"📌 [{detail_label}] 동 상세 펼치기", expanded=bool(dong_in)):
                        r2 = detail_row
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 건물명 | {safe_val(r2.get('bldNm'))} |
| 동명칭 | {safe_val(r2.get('dongNm'))} |
| 대지위치 | {safe_val(r2.get('platPlc'))} |
| 도로명주소 | {safe_val(r2.get('newPlatPlc'))} |
| 주용도 | {safe_val(r2.get('mainPurpsCdNm'))} |
| 기타용도 | {safe_val(r2.get('etcPurps'))} |
| 대장구분 | {safe_val(r2.get('regstrGbCdNm'))} |
| 대장종류 | {safe_val(r2.get('regstrKindCdNm'))} |
                            """)
                        with col2:
                            st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 주구조 | {safe_val(r2.get('strctCdNm'))} |
| 지붕 | {safe_val(r2.get('roofCdNm'))} |
| 규모 | 지하 {safe_val(r2.get('ugrndFlrCnt'),'0')}층 / 지상 {safe_val(r2.get('grndFlrCnt'),'0')}층 |
| 높이 | {safe_val(r2.get('heit'))} m |
| 대지면적 | {safe_val(r2.get('platArea'))} ㎡ |
| 건축면적 | {safe_val(r2.get('archArea'))} ㎡ |
| 연면적 | {safe_val(r2.get('totArea'))} ㎡ |
| 용적률 산정 연면적 | {safe_val(r2.get('vlRatEstmTotArea'))} ㎡ |
                            """)
                        col3, col4 = st.columns(2)
                        with col3:
                            st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 건폐율 | {safe_val(r2.get('bcRat'))} % |
| 용적률 | {safe_val(r2.get('vlRat'))} % |
| 세대수 | {safe_val(r2.get('hhldCnt'),'0')} 세대 |
| 가구수 | {safe_val(r2.get('fmlyCnt'),'0')} 가구 |
| 승용승강기 | {safe_val(r2.get('rideUseElvtCnt'),'0')} 대 |
| 비상승강기 | {safe_val(r2.get('emgenUseElvtCnt'),'0')} 대 |
                            """)
                        with col4:
                            st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 허가일 | {fmt_date(r2.get('pmsDay'))} |
| 착공일 | {fmt_date(r2.get('stcDay'))} |
| 사용승인일 | {fmt_date(r2.get('useAprDay'))} |
| 내진설계 적용 | {safe_val(r2.get('rsthqAbltyYn'))} |
| 내진능력 | {safe_val(r2.get('rsthqAblty'))} |
| 에너지효율등급 | {safe_val(r2.get('engrGrade'))} |
| 친환경건축물등급 | {safe_val(r2.get('gnBldGrade'))} |
| 지능형건축물등급 | {safe_val(r2.get('itgBldGrade'))} |
                            """)

                # ════════════════════════════════════════════
                # 섹션 C: 전유공용면적 (호수 입력 시)
                # 조건: if 독립 사용 (elif 제거 → 항상 표제부 표시됨)
                # ════════════════════════════════════════════
                if ho_in:
                    st.markdown("---")
                    st.markdown(f"### 🚪 전유공용면적 — {dong_in+'동 ' if dong_in else ''}{ho_in}호")
                    if df_expos.empty:
                        st.warning(
                            "⚠️ 전유공용면적 데이터를 찾을 수 없습니다.\n\n"
                            "**확인 사항:**\n"
                            "- 집합건물(아파트·오피스텔·다세대)만 전유부 데이터가 존재합니다\n"
                            "- 동 번호 형식 확인 (예: 101동 → `101` 입력)\n"
                            "- 호수 형식 확인 (예: 502호 → `502` 입력)"
                        )
                    else:
                        for pk in df_expos["mgmBldrgstPk"].unique() if "mgmBldrgstPk" in df_expos.columns else [None]:
                            grp = df_expos[df_expos["mgmBldrgstPk"] == pk] if pk else df_expos

                            # 전유 / 공용 분리
                            if "exposPubuseGbCdNm" in grp.columns:
                                df_j = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("전유", na=False)]
                                df_g = grp[grp["exposPubuseGbCdNm"].astype(str).str.contains("공용", na=False)]
                            else:
                                df_j, df_g = grp, pd.DataFrame()
                            if df_j.empty: df_j = grp

                            j_area = sum(_to_float(x) for x in df_j.get("area", []))
                            g_area = sum(_to_float(x) for x in df_g.get("area", [])) if not df_g.empty else 0.0
                            t_area = j_area + g_area
                            mr = df_j.iloc[0]
                            full_nm = " ".join(filter(None, [
                                safe_val(mr.get("bldNm"),""), safe_val(mr.get("dongNm"),""), safe_val(mr.get("hoNm"),"")
                            ]))

                            with st.container(border=True):
                                st.markdown(f"#### {full_nm}")
                                st.warning("🔒 소유자 정보는 개인정보 보호로 제공되지 않습니다.")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown(f"""
| 항목 | 내용 |
|---|---|
| 주용도 | {safe_val(mr.get('mainPurpsCdNm'))} |
| 기타용도 | {safe_val(mr.get('etcPurps'))} |
| 구조 | {safe_val(mr.get('strctCdNm'))} |
| 해당 층 | {safe_val(mr.get('flrNoNm'))} |
                                    """)
                                with col2:
                                    st.markdown(f"""
| 면적 구분 | 면적 |
|---|---|
| **전용면적** | **{j_area:,.2f} ㎡** |
| **공용면적** | {g_area:,.2f} ㎡ |
| **계약면적(합계)** | **{t_area:,.2f} ㎡** |
| 평형 환산 | 약 {t_area/3.3058:.1f} 평 |
                                    """)
                                # 전유/공용 상세 내역
                                if not grp.empty and "exposPubuseGbCdNm" in grp.columns:
                                    with st.expander("면적 상세 내역"):
                                        detail_df = grp[["exposPubuseGbCdNm","mainPurpsCdNm","area"]].copy()
                                        detail_df.columns = ["전유/공용","용도","면적(㎡)"]
                                        st.dataframe(detail_df, use_container_width=True)

                # ════════════════════════════════════════════
                # 섹션 D: 층별개요
                # ════════════════════════════════════════════
                st.markdown("---")
                st.markdown(f"### 🪜 층별개요{'  —  '+dong_in+'동' if dong_in else ''}")
                if df_floor.empty:
                    st.info("층별개요 데이터가 없습니다. 동 이름을 입력하면 해당 동 층별 현황을 조회합니다.")
                else:
                    floor_cols = {
                        "dongNm":        "동명칭",
                        "flrNoNm":       "층",
                        "mainPurpsCdNm": "주용도",
                        "strctCdNm":     "구조",
                        "area":          "면적(㎡)",
                    }
                    ex_cols = {k:v for k,v in floor_cols.items() if k in df_floor.columns}
                    df_fl_disp = df_floor[list(ex_cols.keys())].rename(columns=ex_cols).copy()
                    df_fl_disp.index = range(1, len(df_fl_disp)+1)
                    st.caption(f"총 {len(df_fl_disp)}개 층 데이터")
                    st.dataframe(df_fl_disp, use_container_width=True)

                st.markdown("---")
                st.caption("※ 본 정보는 국토교통부 건축HUB 건축물대장정보 API 기반이며, 법적 효력이 없습니다. 공식 증명서는 정부24 또는 세움터에서 발급받으세요.")
