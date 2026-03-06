import requests
import xmltodict

MOLIT_API_KEY = "z92CW%2FlIVtpHa46lUJJ5WCMBVQEu8C8YQS9sY2nFsG3nKq0S2J4W997c7ENV6x02Rsnf6RKJcY1hc8cLc2OlxQ%3D%3D"
URL_EXPOS = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo"

# 상도동래미안1차 기준값
SGG_CD    = "11590"
BJD_CD    = "10200"
PLAT_GB   = "0"
BUN       = "0450"
JI        = "0000"
HO_NM     = "501호"
DONG_NM   = "103동"
FL_NO     = "5"   # 501호는 5층

def query(label, params):
    base = (
        f"{URL_EXPOS}?serviceKey={MOLIT_API_KEY}"
        f"&sigunguCd={SGG_CD}&bjdongCd={BJD_CD}"
        f"&platGbCd={PLAT_GB}&bun={BUN}&ji={JI}"
        f"&numOfRows=10&pageNo=1"
    )
    for k, v in params.items():
        base += f"&{k}={v}"
    try:
        res  = requests.get(base, timeout=15)
        data = xmltodict.parse(res.content)
        body = data.get("response", {}).get("body", {})
        cnt  = body.get("totalCount", "파싱실패")
        items = body.get("items", {})
        if items:
            item = items.get("item", [])
            if isinstance(item, dict):
                item = [item]
            sample = [(x.get("dongNm","?"), x.get("hoNm","?")) for x in item[:3]]
        else:
            sample = []
        print(f"\n[{label}]")
        print(f"  totalCount : {cnt}")
        print(f"  샘플(동,호): {sample}")
    except Exception as e:
        print(f"\n[{label}] 오류: {e}")

# 테스트 1: 파라미터 없음 (기준 확인)
query("기준 (파라미터 없음)", {})

# 테스트 2: hoNm 단독
query("hoNm 단독", {"hoNm": HO_NM})

# 테스트 3: dongNm 단독
query("dongNm 단독", {"dongNm": DONG_NM})

# 테스트 4: dongNm + hoNm 조합
query("dongNm+hoNm 조합", {"dongNm": DONG_NM, "hoNm": HO_NM})

# 테스트 5: flrNo 단독
query("flrNo 단독", {"flrNo": FL_NO})

# 테스트 6: flrNo + hoNm
query("flrNo+hoNm", {"flrNo": FL_NO, "hoNm": HO_NM})
