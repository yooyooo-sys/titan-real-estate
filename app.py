import streamlit as st
import pandas as pd
import requests
import xmltodict
import time
from io import BytesIO

# --- 1. API 키 설정 ---
DONG_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"

# --- 2. 매물 및 거래 종류별 국토부 API 주소 (실거래가용) ---
API_PATHS = {
    "아파트_매매": "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "아파트_전월세": "RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "아파트분양권_매매": "RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade", 
    "오피스텔_매매": "RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "오피스텔_전월세": "RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "연립/다세대_매매": "RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "연립/다세대_전월세": "RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "단독/다가구_매매": "RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "단독/다가구_전월세": "RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    "상업/업무용_매매": "RTMSDataSvcNrgTrade/getRTMSDataSvcNrgTrade",
    "공장 및 창고_매매": "RTMSDataSvcInduTrade/getRTMSDataSvcInduTrade", 
    "토지_매매": "RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
}

# ==========================================
# 🌟 [기능 1] 실거래가 데이터 처리 함수들
# ==========================================
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
            active_regions = [row for row in rows if row["sido_cd"] != "" and row["sgg_cd"] != ""]
            for region in active_regions:
                full_address = region["locatadd_nm"]
                if sigungu_name.strip() in full_address:
                    return region["region_cd"][:5], full_address
        return None, None
    except:
        return None, None

def get_real_estate_data(sigungu_code, start_month, end_month, dong_name, prop_type, trans_type):
    dict_key = f"{prop_type}_{trans_type}"
    if dict_key not in API_PATHS:
        st.warning(f"⚠️ '{prop_type} {trans_type}' 조합은 제공하지 않거나 불가능한 거래입니다.")
        return pd.DataFrame()
        
    api_path = API_PATHS[dict_key]
    base_url = f"http://apis.data.go.kr/1613000/{api_path}"
    
    try:
        start_date = pd.to_datetime(start_month, format="%Y%m")
        end_date = pd.to_datetime(end_month, format="%Y%m")
        month_list = pd.date_range(start_date, end_date, freq='MS').strftime("%Y%m").tolist()
    except:
        st.error("조회 기간 형식이 잘못되었습니다. YYYYMM 형식으로 입력해주세요.")
        return pd.DataFrame()

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    headers = {'User-Agent': 'Mozilla/5.0'}

    for i, target_month in enumerate(month_list):
        status_text.text(f"⏳ {target_month} 실거래가 데이터를 가져오는 중입니다... ({i+1}/{len(month_list)})")
        progress_bar.progress((i + 1) / len(month_list))
        
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={sigungu_code}&DEAL_YMD={target_month}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            content = response.text.strip()
            if not content.startswith('<'):
                st.error(f"🚨 국토부 서버 응답 지연 ({target_month})")
                break
            xml_data = xmltodict.parse(response.content)
            if 'OpenAPI_ServiceResponse' in xml_data: break
            if xml_data.get('response', {}).get('header', {}).get('resultCode') not in ['00', '0', '200', '000']: continue
                
            items_dict = xml_data.get('response', {}).get('body', {}).get('items')
            if items_dict and 'item' in items_dict:
                item_list = items_dict['item']
                if isinstance(item_list, dict): item_list = [item_list]
                all_data.append(pd.DataFrame(item_list))
        except:
            continue
        time.sleep(0.3)
            
    status_text.empty()
    progress_bar.empty()

    if not all_data:
        st.warning("선택하신 기간 동안 거래된 내역이 없거나, 조회가 중단되었습니다.")
        return pd.DataFrame()
        
    df = pd.concat(all_data, ignore_index=True)
    if dong_name.strip(): df = df[df['umdNm'].str.contains(dong_name.strip(), na=False)]
    if df.empty: 
        st.warning(f"'{dong_name}' 지역에는 해당 기간 동안 거래된 내역이 없습니다.")
        return pd.DataFrame()
        
    df = df.rename(columns={
        'dealYear': '년', 'dealMonth': '월', 'dealDay': '일', 'umdNm': '법정동', 'jibun': '지번',
        'aptNm': '건물명', 'offiNm': '건물명', 'mviNm': '건물명', 'bldgNm': '건물명', '단지': '건물명', 
        'rletTypeNm': '건물유형', 'rletTpNm': '건물유형', 'buildingType': '건물유형',
        'purpsRgnNm': '용도지역', 'prpsRgnNm': '용도지역', 'landUse': '용도지역',
        'excluUseAr': '전용면적', 'area': '계약면적', 'dealArea': '거래면적', 
        'bldgMarea': '건물면적', 'blgMarea': '건물면적', 'bldgArea': '건물면적', 'buildingAr': '건물면적',
        'plArea': '대지면적', 'platArea': '대지면적', 'totArea': '연면적', 'plottageAr': '대지면적',
        'dealAmount': '거래금액', 'deposit': '보증금', 'monthlyRent': '월세', 
        'floor': '층', 'flr': '층', 'jimok': '지목', 'buildYear': '건축년도', 
        'reqGbn': '거래유형', 'dealingGbn': '거래유형', 'cnclYmd': '계약취소일', 'cdealDay': '계약취소일',
        'estbDvsnNm': '중개사소재지', 'estateAgentSggNm': '중개사소재지', 'buildingUse': '건물주용도', 
        'buyerGbn': '매수자', 'slerGbn': '매도자', 'shareDealingType': '지분거래여부', 'sggNm': '시군구'
    })
    
    if '법정동' in df.columns and '지번' in df.columns:
        df['소재지'] = df['법정동'] + " " + df['지번'].fillna('').astype(str).str.strip()
    elif '법정동' in df.columns:
        df['소재지'] = df['법정동']

    if all(x in df.columns for x in ['년', '월', '일']):
        df['계약일'] = df['년'].astype(str) + "-" + df['월'].astype(str).str.zfill(2) + "-" + df['일'].astype(str).str.zfill(2)
    
    if trans_type == "매매" and '거래금액' in df.columns:
        area_cols = ['전용면적', '건물면적', '연면적', '거래면적', '대지면적', '계약면적']
        available_area_col = next((col for col in area_cols if col in df.columns), None)
        if available_area_col:
            def calc_pyeong_price(row):
                try:
                    price = int(str(row['거래금액']).replace(',', '').strip()) 
                    area = float(str(row[available_area_col]).replace(',', '').strip()) 
                    if area <= 0: return ""
                    price_per_pyeong = int(price / (area / 3.3058))
                    uk, man = price_per_pyeong // 10000, price_per_pyeong % 10000
                    if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
                    return f"{price_per_pyeong}만원"
                except: return ""
            df['평당가격'] = df.apply(calc_pyeong_price, axis=1)

    display_cols = ['계약일', '소재지', '건물유형', '용도지역', '건물주용도', '건물명', '건축년도', '대지면적', '건물면적', '연면적', '전용면적', '층', '거래금액', '평당가격', '매수자', '매도자', '지분거래여부', '거래유형', '중개사소재지', '계약취소일']
    final_cols = [c for c in display_cols if c in df.columns]
    result_df = df[final_cols].copy()
    
    def format_money(price_str):
        if pd.isna(price_str): return ""
        try:
            price = int(str(price_str).replace(',', '').strip())
            uk, man = price // 10000, price % 10000
            if uk > 0: return f"{uk}억 {man}만원" if man > 0 else f"{uk}억원"
            return f"{price}만원"
        except: return price_str
        
    for col in ['거래금액', '보증금']:
        if col in result_df.columns: result_df[col] = result_df[col].apply(format_money)
            
    if '계약일' in result_df.columns: result_df = result_df.sort_values(by='계약일', ascending=False)
    return result_df


# ==========================================
# 🌟 [기능 2] 건축물대장 처리 함수 (V21: 무적의 에러 방어막 탑재)
# ==========================================
import re 
import pandas as pd
import requests
import xmltodict
import time
from io import BytesIO
import streamlit as st

def parse_address_for_bldrgst(address_str):
    parts = address_str.strip().split()
    if not parts: return None, None, None, None
        
    plat_gb_cd = "0" 
    if "산" in parts:
        plat_gb_cd = "1"
        parts.remove("산")
        
    bun, ji = "0000", "0000"
    last_part = parts[-1]
    
    if any(char.isdigit() for char in last_part):
        if "-" in last_part:
            b, j = last_part.split("-", 1)
            b, j = ''.join(filter(str.isdigit, b)), ''.join(filter(str.isdigit, j))
            bun = b.zfill(4) if b else "0000"
            ji = j.zfill(4) if j else "0000"
        else:
            b = ''.join(filter(str.isdigit, last_part))
            bun = b.zfill(4) if b else "0000"
        parts.pop() 
        
    region_search_term = " ".join(parts) 
    return region_search_term, plat_gb_cd, bun, ji

def get_full_bjdong_code(search_term):
    base_url = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
    url = f"{base_url}?serviceKey={DONG_API_KEY}&pageNo=1&numOfRows=10&type=json&locatadd_nm={search_term}"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("StanReginCd"):
            rows = data["StanReginCd"][1]["row"]
            active_regions = [row for row in rows if row["sido_cd"] != "" and row["sgg_cd"] != ""]
            if active_regions:
                region_cd = active_regions[0]["region_cd"]
                return region_cd[:5], region_cd[5:10], active_regions[0]["locatadd_nm"]
        return None, None, None
    except: return None, None, None

def fetch_molit_api(base_url, sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, status_text, desc_prefix, max_pages=50):
    all_items = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}?serviceKey={MOLIT_API_KEY}&sigunguCd={sgg_cd}&bjdongCd={bjdong_cd}&platGbCd={plat_gb_cd}&bun={bun}&ji={ji}&numOfRows=1000&pageNo={page}"
        if status_text:
            status_text.info(f"⏳ {desc_prefix} 수집 중... ({page}페이지)")
        
        success = False
        xml_data = {}
        for _ in range(3):
            try:
                res = requests.get(url, timeout=15)
                if res.text.strip().startswith('<'):
                    xml_data = xmltodict.parse(res.content)
                    if 'OpenAPI_ServiceResponse' not in xml_data:
                        success = True
                        break
            except: time.sleep(1)
        if not success: break
        
        body = xml_data.get('response', {}).get('body', {})
        items = body.get('items', {}).get('item', []) if body.get('items') else []
        if isinstance(items, dict): items = [items]
        if not items: break
        
        all_items.extend(items)
        total_count = int(body.get('totalCount', 0))
        if page * 1000 >= total_count: break
    return all_items

def get_comprehensive_ledger(sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, target_dong="", target_ho=""):
    plat_candidates = ['3', '2', '0'] if plat_gb_cd != '1' else ['1']
    status_text = st.empty()
    
    URL_BASIS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo" 
    URL_TITLE = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"     
    URL_EXPOS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrExposInfo"     
    URL_FLOOR = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrFlrOulnInfo"     

    def match_dong(t_dong, d_val, b_val):
        if not t_dong: return True
        t_clean = ''.join(filter(str.isalnum, str(t_dong))).upper().replace('동','').replace('제','')
        d_clean = ''.join(filter(str.isalnum, str(d_val))).upper().replace('동','').replace('제','')
        b_clean = ''.join(filter(str.isalnum, str(b_val))).upper()
        if t_clean == d_clean or f"{t_clean}동" in b_clean: return True
        
        t_nums = re.findall(r'\d+', t_clean)
        if t_nums:
            t_num = t_nums[-1]
            short_num = str(int(t_num) % 100)
            if d_clean in [f"주{short_num}", str(short_num), f"주{t_num}", t_num]: return True
            if f"{t_num}동" in b_clean: return True
        return False

    def match_ho(h_target, h_val):
        if not h_target: return True
        t_clean = ''.join(filter(str.isalnum, str(h_target))).upper().replace('호','').replace('제','')
        h_clean = ''.join(filter(str.isalnum, str(h_val))).upper().replace('호','').replace('제','')
        if t_clean == h_clean: return True
        
        t_nums = re.findall(r'\d+', t_clean)
        h_nums = re.findall(r'\d+', h_clean)
        if t_nums and h_nums and t_nums[-1] == h_nums[-1]: return True
        return False

    # 1️⃣ 표제부 탐색 (다중 PK 완벽 수집)
    target_pks = set()
    found_plat_gb = plat_gb_cd
    title_records = []
    pk_map = {}
    
    def restore(r, k):
        v = r.get(k)
        if not v or str(v).strip() in ['None','nan','']:
            pk = r.get('mgmBldrgstPk')
            if pk and pk in pk_map: return pk_map[pk].get('dong' if k=='dongNm' else 'bld')
        return v

    for p_gb in plat_candidates:
        items = fetch_molit_api(URL_TITLE, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 표제부 스캔", max_pages=10)
        if not items: continue
        
        for item in items:
            tot = str(item.get('totArea', '0')).replace(',', '')
            try: tot_f = float(tot)
            except: tot_f = 0.0
            if tot_f <= 0.0: continue 
            
            title_records.append(item)
            pk = item.get('mgmBldrgstPk')
            d_nm = item.get('dongNm', '')
            b_nm = item.get('bldNm', '')
            if pk: pk_map[pk] = {'dong': d_nm, 'bld': b_nm}
            
            if target_dong and match_dong(target_dong, d_nm, b_nm):
                target_pks.add(pk)
                
        if target_pks: 
            found_plat_gb = p_gb
            break 
        elif not target_dong and title_records:
            found_plat_gb = p_gb
            break

    # 2️⃣ 총괄표제부 탐색 
    df_basis = pd.DataFrame()
    for p_gb in ['0', '2', '3', '1']: 
        basis_items = fetch_molit_api(URL_BASIS, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 총괄표제부 탐색", max_pages=2)
        valid_basis = [b for b in basis_items if float(str(b.get('totArea', '0')).replace(',', '') or 0) > 0.0]
        if valid_basis:
            df_basis = pd.DataFrame(valid_basis)
            break 

    # 3️⃣ 전유부 탐색 
    df_expos = pd.DataFrame()
    is_missing_area = False
    
    if target_ho:
        for p_gb in plat_candidates:
            expos_items = fetch_molit_api(URL_EXPOS, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 전유부 정밀 추적", max_pages=50)
            if not expos_items: continue
            
            temp_df = pd.DataFrame(expos_items)
            temp_df['dongNm'] = temp_df.apply(lambda r: restore(r, 'dongNm'), axis=1)
            temp_df['bldNm'] = temp_df.apply(lambda r: restore(r, 'bldNm'), axis=1)
            
            matched_ho = temp_df[temp_df.apply(lambda r: match_ho(target_ho, r.get('hoNm')), axis=1)]
            
            if target_dong and not matched_ho.empty:
                matched_ho = matched_ho[matched_ho.apply(lambda r: r.get('mgmBldrgstPk') in target_pks or match_dong(target_dong, r.get('dongNm'), r.get('bldNm')), axis=1)]
            
            if not matched_ho.empty:
                df_expos = matched_ho
                if 'area' not in df_expos.columns: 
                    df_expos['area'] = '0'
                    is_missing_area = True
                break

    # 4️⃣ 층별개요 탐색 
    df_floor = pd.DataFrame()
    if target_dong:
        for p_gb in plat_candidates:
            floor_items = fetch_molit_api(URL_FLOOR, sgg_cd, bjdong_cd, p_gb, bun, ji, status_text, f"[지번코드 {p_gb}] 층별현황 추적", max_pages=50)
            if not floor_items: continue
            
            temp_floor = pd.DataFrame(floor_items)
            if target_pks:
                temp_floor = temp_floor[temp_floor['mgmBldrgstPk'].isin(target_pks)]
            else:
                temp_floor['dongNm'] = temp_floor.apply(lambda r: restore(r, 'dongNm'), axis=1)
                temp_floor['bldNm'] = temp_floor.apply(lambda r: restore(r, 'bldNm'), axis=1)
                temp_floor = temp_floor[temp_floor.apply(lambda r: match_dong(target_dong, r.get('dongNm'), r.get('bldNm')), axis=1)]
                
            if not temp_floor.empty:
                df_floor = temp_floor
                if 'flrNo' in df_floor.columns:
                    df_floor['flrNo_num'] = pd.to_numeric(df_floor['flrNo'], errors='coerce').fillna(0)
                    df_floor = df_floor.sort_values(by='flrNo_num', ascending=False).drop(columns=['flrNo_num'])
                break 

    status_text.empty()
    return df_basis, pd.DataFrame(title_records), df_expos, df_floor, is_missing_area

# ==========================================
# 🌟 [UI 구성] 웹 화면 (탭 분리)
# ==========================================
st.set_page_config(page_title="부동산 올인원 봇", layout="wide")
st.title("🏢 부동산 올인원 실거래가 & 건축물대장 봇")

tab1, tab2 = st.tabs(["💰 실거래가 조회", "📋 종합 건축물대장 (단지/동/호수/층별)"])

# ----------------- [탭 1] 실거래가 (생략: 기존 유지) -----------------
with tab1:
    current_date = pd.Timestamp.now()
    current_month_str = current_date.strftime('%Y%m') 
    prev_month_str = (current_date - pd.DateOffset(months=1)).strftime('%Y%m') 

    with st.form("search_form"):
        col1, col2 = st.columns(2)
        with col1: property_type = st.selectbox("매물 종류", ["아파트", "아파트분양권", "오피스텔", "연립/다세대", "단독/다가구", "상업/업무용", "공장 및 창고", "토지"])
        with col2: transaction_type = st.selectbox("거래 종류", ["매매", "전월세"])
            
        col3, col4, col5, col6 = st.columns(4)
        with col3: sigungu_name = st.text_input("시/군/구 (예: 서초구)", value="서초구")
        with col4: dong_name = st.text_input("법정동 (빈칸 시 구 전체 조회)", value="")
        with col5: start_month = st.text_input("시작 월 (예: 202301)", value=prev_month_str)
        with col6: end_month = st.text_input("종료 월 (예: 202406)", value=current_month_str)
            
        submitted = st.form_submit_button("🔍 전체 기간 실거래가 조회하기")

    if submitted:
        if not sigungu_name: st.warning("시/군/구 이름은 반드시 입력해주세요.")
        else:
            sigungu_code, full_region_name = get_sigungu_code(sigungu_name, dong_name)
            if sigungu_code:
                st.success(f"✅ 지역 변환 성공: {full_region_name} ({sigungu_code})")
                real_data_df = get_real_estate_data(sigungu_code, start_month, end_month, dong_name, property_type, transaction_type)
                if not real_data_df.empty:
                    real_data_df.index = range(1, len(real_data_df) + 1)
                    st.dataframe(real_data_df, use_container_width=True)
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        real_data_df.to_excel(writer, index=True, index_label='순번', sheet_name='실거래가')
                    st.download_button("📥 엑셀 파일로 다운로드", data=excel_buffer.getvalue(), file_name=f"실거래가.xlsx")
            else:
                st.error("지역을 찾을 수 없습니다.")

# ----------------- [탭 2] 건축물대장 
with tab2:
    st.subheader("📋 특정 지번 건축물대장 종합 조회")
    st.info("💡 단지 정보, 해당 층별 정보, 전유부 면적을 한 번의 검색으로 모두 분석합니다.")
    
    with st.form("bldrgst_form"):
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            address_input = st.text_input("조회할 텍스트 주소 (필수)", placeholder="예: 상도동 450")
        with col2:
            dong_input = st.text_input("동 (선택)", placeholder="예: 105")
        with col3:
            ho_input = st.text_input("호수 (선택)", placeholder="예: 304")
            
        bld_submitted = st.form_submit_button("🔍 종합 대장 데이터 열람")
        
    if bld_submitted and address_input:
        region_search_term, plat_gb_cd, bun, ji = parse_address_for_bldrgst(address_input)
        if not region_search_term:
            st.warning("주소를 올바르게 입력해주세요.")
        else:
            sgg_cd, bjdong_cd, full_loc_name = get_full_bjdong_code(region_search_term)
            if sgg_cd and bjdong_cd:
                st.success(f"✅ 주소 파싱 성공: {full_loc_name} (본번:{bun} 부번:{ji})")
                
                df_basis, df_title, df_expos, df_floor, is_missing_area = get_comprehensive_ledger(
                    sgg_cd, bjdong_cd, plat_gb_cd, bun, ji, dong_input, ho_input
                )
                
                if df_title.empty and df_expos.empty:
                    st.warning(f"🚨 조회 결과가 없습니다. 지번이나 동/호수를 다시 한번 확인해주세요.")
                else:
                    st.markdown("<br>", unsafe_allow_html=True)
                    def safe_val(val, default='-'):
                        return default if pd.isna(val) or str(val).strip() in ['None', '', 'nan'] else str(val).strip()

                    # 🚨 위반건축물 감지 로직 (KeyError 완벽 방어막 적용!)
                    is_violating = False
                    if not df_title.empty and 'violBldYn' in df_title.columns:
                        if '1' in df_title['violBldYn'].astype(str).values:
                            is_violating = True
                    elif not df_basis.empty and 'violBldYn' in df_basis.columns:
                        if '1' in df_basis['violBldYn'].astype(str).values:
                            is_violating = True

                    if is_violating:
                        st.error("🚨 **[주의] 위반건축물로 등록된 대장입니다!** 해당 건축물은 건축법 위반 사항이 존재하므로 중개 시 반드시 정부24 원본 서류를 확인하여 위반 내용을 확인해야 합니다.")

                    # 🟩 [섹션 1] 총괄표제부 (KeyError 완벽 방어막 적용!)
                    if not df_basis.empty:
                        tot_area_series = df_basis['totArea'] if 'totArea' in df_basis.columns else pd.Series('0', index=df_basis.index)
                        df_basis['totArea_num'] = pd.to_numeric(tot_area_series.astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        
                        valid_basis = df_basis[df_basis['totArea_num'] > 0]
                        row_b = valid_basis.iloc[0] if not valid_basis.empty else df_basis.iloc[0]

                        st.markdown(f"### 🏢 [총괄표제부] 단지 전체 개요")
                        st.markdown(f"#### 📍 {safe_val(row_b.get('bldNm'), '명칭 없음')}")
                        st.markdown(f"""
                        | 구분 | 상세 내용 | 구분 | 상세 내용 |
                        |:---:|---|:---:|---|
                        | **대지면적** | {safe_val(row_b.get('platArea'))} ㎡ | **주용도** | {safe_val(row_b.get('mainPurpsCdNm'))} |
                        | **연면적(총)** | {safe_val(row_b.get('totArea'))} ㎡ | **총 세대/가구** | {safe_val(row_b.get('hhldCnt'),'0')}세대 / {safe_val(row_b.get('fmlyCnt'),'0')}가구 |
                        | **건폐율/용적률** | {safe_val(row_b.get('bcRat'))}% / {safe_val(row_b.get('vlRat'))}% | **총 주차대수** | {safe_val(row_b.get('totPkngCnt'),'0')} 대 |
                        """, unsafe_allow_html=True)
                        st.divider()

                    # 🟩 [섹션 2] 전유부 
                    if ho_input and not df_expos.empty:
                        st.markdown(f"### 🏠 [전유부] 해당 호수 상세")
                        unique_pks = df_expos['mgmBldrgstPk'].unique() if 'mgmBldrgstPk' in df_expos.columns else [None]
                        
                        for pk in unique_pks[:3]:
                            grp = df_expos[df_expos['mgmBldrgstPk'] == pk] if pk else df_expos
                            
                            if 'exposPubuseGbCdNm' in grp.columns:
                                is_j = grp['exposPubuseGbCdNm'].astype(str).str.contains('전유', na=False)
                                is_g = grp['exposPubuseGbCdNm'].astype(str).str.contains('공용', na=False)
                                df_j = grp[is_j]
                                df_g = grp[is_g]
                            else:
                                df_j = grp
                                df_g = pd.DataFrame()
                                
                            if df_j.empty: df_j = grp
                            
                            def s_float(v):
                                try: return float(str(v).replace(',', '').strip())
                                except: return 0.0
                                
                            j_area = sum(s_float(x) for x in df_j.get('area', [])) if not is_missing_area else 0.0
                            g_area = sum(s_float(x) for x in df_g.get('area', [])) if not is_missing_area else 0.0
                            t_area = j_area + g_area
                            
                            m_row = df_j.iloc[0]
                            full_nm = " ".join([x for x in [safe_val(m_row.get('bldNm'),''), safe_val(m_row.get('dongNm'),''), safe_val(m_row.get('hoNm'),'')] if x])
                            
                            with st.container(border=True):
                                st.markdown(f"#### 🚪 명칭: {full_nm}")
                                
                                j_str = "<span style='color:red; font-size:0.9em;'>⚠️ API 전산 누락</span>" if is_missing_area else f"<span style='color:#0066cc; font-weight:bold; font-size:1.1em;'>{j_area:,.2f} ㎡</span>"
                                g_str = "<span style='color:red; font-size:0.9em;'>⚠️ 누락</span>" if is_missing_area else f"{g_area:,.2f} ㎡"
                                t_str = "<span style='color:#d93025; font-weight:bold;'>확인 불가</span>" if is_missing_area else f"<span style='color:#d93025; font-weight:bold; font-size:1.1em;'>{t_area:,.2f} ㎡</span>"

                                st.markdown(f"""
                                | 구분 | 상세 내용 | 구분 | 상세 내용 |
                                |:---:|---|:---:|---|
                                | **주용도** | {safe_val(m_row.get('mainPurpsCdNm'))} | **해당 층** | {safe_val(m_row.get('flrNoNm'))} |
                                | **전용면적** | {j_str} | **기타용도** | {safe_val(m_row.get('etcPurps'))} |
                                | **공용면적** | {g_str} | **구조** | {safe_val(m_row.get('strctCdNm'))} |
                                | **계약면적(총)**| {t_str} | **대지권지분** | 등기부등본 확인 요망 |
                                """, unsafe_allow_html=True)
                        st.divider()

                    elif not ho_input and not df_title.empty:
                        m_row = df_title.iloc[0]
                        d_nm = f" {safe_val(m_row.get('dongNm'))}" if safe_val(m_row.get('dongNm')) != '-' else ""
                        full_nm = f"{safe_val(m_row.get('bldNm'), '')}{d_nm}"
                        
                        st.markdown(f"### 📄 [표제부] 해당 동/건물 상세")
                        with st.container(border=True):
                            if full_nm.strip(): st.markdown(f"#### 🏢 명칭: {full_nm}")
                            st.markdown(f"""
                            | 구분 | 상세 내용 | 구분 | 상세 내용 |
                            |:---:|---|:---:|---|
                            | **주용도** | {safe_val(m_row.get('mainPurpsCdNm'))} | **규모** | 지하 {safe_val(m_row.get('ugrndFlrCnt'),'0')}층 / 지상 {safe_val(m_row.get('grndFlrCnt'),'0')}층 |
                            | **대지면적** | {safe_val(m_row.get('platArea'))} ㎡ | **구조** | {safe_val(m_row.get('strctCdNm'))} |
                            | **연면적** | {safe_val(m_row.get('totArea'))} ㎡ | **승강기** | 승용 {safe_val(m_row.get('rideUseElvtCnt'),'0')} / 비상 {safe_val(m_row.get('emgenUseElvtCnt'),'0')} |
                            """, unsafe_allow_html=True)
                        st.divider()

                    # 🟩 [섹션 3] 층별 현황
                    if not df_floor.empty:
                        st.markdown(f"### 🪜 [층별개요] 해당 동 층별 구조/면적 현황")
                        df_f_clean = df_floor[['flrNoNm', 'strctCdNm', 'mainPurpsCdNm', 'area']].rename(
                            columns={'flrNoNm':'해당 층', 'strctCdNm':'구조', 'mainPurpsCdNm':'주용도', 'area':'면적(㎡)'}
                        )
                        st.dataframe(df_f_clean, use_container_width=True)

            else:
                st.error("해당하는 지역을 찾을 수 없습니다.")