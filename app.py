import streamlit as st
import requests
import xmltodict

MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
URL_EXPOS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo"

SGG_CD  = "11590"
BJD_CD  = "10200"
PLAT_GB = "0"
BUN     = "0450"
JI      = "0000"
HO_NM   = "501호"
DONG_NM = "103동"
FL_NO   = "5"

st.title("🧪 전유공용 API 파라미터 지원 테스트")

def query(label, extra_params):
    url = (
        f"{URL_EXPOS}?serviceKey={MOLIT_API_KEY}"
        f"&sigunguCd={SGG_CD}&bjdongCd={BJD_CD}"
        f"&platGbCd={PLAT_GB}&bun={BUN}&ji={JI}"
        f"&numOfRows=10&pageNo=1"
    )
    for k, v in extra_params.items():
        url += f"&{k}={v}"
    try:
        res   = requests.get(url, timeout=15)
        data  = xmltodict.parse(res.content)
        body  = data.get("response", {}).get("body", {})
        cnt   = body.get("totalCount", "파싱실패")
        items = body.get("items", {})
        sample = []
        if items:
            item = items.get("item", [])
            if isinstance(item, dict):
                item = [item]
            sample = [(x.get("dongNm","?"), x.get("hoNm","?")) for x in item[:3]]
        st.markdown(f"#### [{label}]")
        col1, col2 = st.columns(2)
        col1.metric("totalCount", cnt)
        col2.write(f"샘플(동,호): {sample}")
        st.divider()
    except Exception as e:
        st.error(f"[{label}] 오류: {e}")

if st.button("🔍 테스트 실행"):
    with st.spinner("API 호출 중..."):
        query("① 기준 (파라미터 없음)",  {})
        query("② hoNm 단독",             {"hoNm": HO_NM})
        query("③ dongNm 단독",            {"dongNm": DONG_NM})
        query("④ dongNm+hoNm 조합",       {"dongNm": DONG_NM, "hoNm": HO_NM})
        query("⑤ flrNo 단독",             {"flrNo": FL_NO})
        query("⑥ flrNo+hoNm",            {"flrNo": FL_NO, "hoNm": HO_NM})
