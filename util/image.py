import requests
import base64
from PIL import Image, ImageStat   # ★ 추가 필요
from io import BytesIO  # ★ 추가 필요


# 외부 사이트 이미지 제한정책으로 인한 이미지 로컬 다운로드 
def encode_image_to_base64(image_url, model_name):
    """
    이미지를 다운로드하여 리사이징 및 압축 후 Base64 리스트로 반환
    - 긴 축 최대 1024px로 리사이징
    - JPEG 품질 70으로 압축
    - 세로로 긴 이미지는 잘라서 처리
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(image_url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            # 1. 이미지 데이터 로드
            img_data = response.content
            
            # ★ [Qwen 에러 방지] 이미지 크기 검사 로직 추가
            try:
                img = Image.open(BytesIO(img_data))
                if img.mode in ("RGBA", "P"): img = img.convert("RGB") # 포맷 통일

                width, height = img.size                
                # 가로 또는 세로가 50px 미만이면 무시 (아이콘, 추적픽셀 등)
                if width < 50 or height < 50:
                    print(f"🚫 너무 작은 이미지 제외 ({width}x{height}): {image_url}")
                    return None
                
                # 리사이징 설정
                MAX_SIZE = 1024
                JPEG_QUALITY = 85
                results = []

                # gemini 아닌 경우만 해상도 조정
                if not "gemini" in model_name.lower():
                    # img.thumbnail((MAX_SIZE, MAX_SIZE), Image.Resampling.LANCZOS)
                    buf = BytesIO()
                    img.save(buf, format="JPEG", quality=JPEG_QUALITY)
                    b64_str = base64.b64encode(buf.getvalue()).decode("utf-8")
                    return f"data:image/jpeg;base64,{b64_str}"

            except Exception:
                # 이미지 파일이 아니거나 손상된 경우 무시
                return None

        # 2. Base64 인코딩
        encoded_string = base64.b64encode(img_data).decode('utf-8')
        
        # 확장자 판별 (기본 jpg)
        mime_type = "image/jpeg"
        if image_url.lower().endswith(".png"):
            mime_type = "image/png"
        elif image_url.lower().endswith(".gif"):
            mime_type = "image/gif"
            
        return f"data:{mime_type};base64,{encoded_string}"
            
    except Exception as e:
        print(f"이미지 다운로드 실패: {e}")
        return None
    return None

# ==========================================
# [2] 이미지 처리 함수 (★ 스마트 컷 기능 추가됨)
# ==========================================

def find_safe_split_point(img, start_y, max_height, lookback_range=200):
    """
    이미지를 자를 때, 자르려는 위치(max_height)에서 위쪽(lookback_range)을 스캔하여
    가장 '단색(여백)'에 가까운 행을 찾아 그 위치를 반환합니다.
    """
    width, total_height = img.size
    
    # 기본적으로 자르려고 했던 위치
    target_cut = start_y + max_height
    
    # 이미 끝부분이면 그대로 반환
    if target_cut >= total_height:
        return total_height
    
    # 검색 시작 위치 (자르려는 곳보다 조금 위에서부터 탐색)
    search_start = max(start_y, target_cut - lookback_range)
    search_end = target_cut
    
    # 해당 구간을 잘라내서 분석
    strip = img.crop((0, search_start, width, search_end))
    strip_gray = strip.convert("L") # 흑백 변환 (계산 단순화)
    
    min_variance = float('inf')
    best_cut_y = target_cut # 못 찾으면 그냥 원래대로 자름
    
    # 아래에서 위로 스캔 (최대한 길게 가져가기 위해)
    # 속도를 위해 5픽셀 단위로 건너뛰며 검사
    for y in range(strip.height - 1, 0, -5):
        # 한 줄(row) 가져오기
        row_img = strip_gray.crop((0, y, width, y+1))
        stat = ImageStat.Stat(row_img)
        
        # 표준편차(stddev)가 0에 가까우면 단색(흰색 배경 등)임
        variance = stat.stddev[0]
        
        # 매우 깨끗한 여백을 발견하면 즉시 그곳을 커팅 포인트로 선정
        if variance < 2.0: 
            best_cut_y = search_start + y
            break
            
        # 가장 여백에 가까운 곳을 기록해둠
        if variance < min_variance:
            min_variance = variance
            best_cut_y = search_start + y
            
    return best_cut_y

# 이미지 chunk
def encode_image_to_base64_chunk(image_url, model_name):
    """
    이미지를 다운로드하여 Base64 리스트로 반환 (긴 이미지는 자름)
    Return: List[str] (예: ["data:...", "data:..."])
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(image_url, headers=headers, timeout=5)
        if response.status_code == 200:
            img_data = response.content
            try:
                img = Image.open(BytesIO(img_data))
                if img.mode in ("RGBA", "P"): img = img.convert("RGB")
                
                width, height = img.size
                if width < 50 or height < 50: return [] 

                MAX_SIZE = 1024
                JPEG_QUALITY = 100 if "gemini" in model_name.lower() else 85
                results = []
                
                # [Case A] 세로로 긴 상세페이지 (높이가 너비의 2배 이상)
                if height > width * 2.0:
                    # 1. 가로 너비를 1024px로 리사이징 (세로 비율 유지)
                    if width > MAX_SIZE:
                        ratio = MAX_SIZE / width
                        new_width = MAX_SIZE
                        new_height = int(height * ratio)
                        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        width, height = img.size

                    # 2. 추출할 조각의 높이 설정 (정사각형에 가깝게)
                    crop_h = width 
                    
                    # 3. 핵심 4지점 좌표 계산 (Top, 1/3지점, 2/3지점, Bottom)
                    # 만약 이미지가 너무 짧아서 4개가 안 나오면 중복 제거됨
                    offsets = [
                        0,                          # [1] 헤드 (메인 이미지)
                        int(height * 0.33),         # [2] 상단부 (모델 착용샷)
                        int(height * 0.66),         # [3] 하단부 (디테일/컬러)
                        max(0, height - crop_h)     # [4] 푸터 (사이즈표/정보)
                    ]
                    
                    # 좌표 중복 제거 및 정렬
                    unique_offsets = sorted(list(set(offsets)))
                    
                    for y in unique_offsets:
                        bottom = min(y + crop_h, height)
                        
                        # 범위 보정 (이미지 끝을 넘어가지 않게)
                        if bottom == height:
                            y = max(0, height - crop_h)
                        
                        cropped = img.crop((0, y, width, bottom))
                        
                        # 전송용 변환
                        buf = BytesIO()
                        cropped.save(buf, format="JPEG", quality=JPEG_QUALITY)
                        b64_str = base64.b64encode(buf.getvalue()).decode('utf-8')
                        results.append(f"data:image/jpeg;base64,{b64_str}")
                        
                    return results # 최대 4장 반환
                
                # [Case B] 일반 비율 이미지
                else:
                    # if width > MAX_SIZE or height > MAX_SIZE:
                        # img.thumbnail((MAX_SIZE, MAX_SIZE), Image.Resampling.LANCZOS)
                    buf = BytesIO()
                    img.save(buf, format="webp", quality=JPEG_QUALITY)
                    b64_str = base64.b64encode(buf.getvalue()).decode('utf-8')
                    return [f"data:image/jpeg;base64,{b64_str}"]
            except Exception: return []
    except Exception: return []
    return []

# html에서 img 링크 추출
def extract_img_for_html(soup, basic_ext_nm, max_images=6):
    found_images = []
    seen_urls = set()
    
    # 모든 img 태그 검색
    for img in soup.find_all('img'):
        src = img.get('src')
        if not src:
            continue
            
        # 절대 경로 변환 (urllib 사용 권장)
        if src.startswith("//"):
            full_url = "https:" + src
        elif src.startswith("/"):
            # base_url이 없는데 상대 경로면 스킵하거나 로직 추가 필요
            continue 
        else:
            full_url = src

        # [필터링 로직]
        # 1. 중복 제거
        if full_url in seen_urls:
            continue
            
        # 2. 아이콘, 로고, 작은 UI 요소 제외 (파일명이나 클래스명으로 1차 필터)
        lower_src = full_url.lower()
        if any(x in lower_src for x in ['logo', 'icon', 'button', 'tracker', 'pixel', 'sns', 'banner']):
            continue

        found_images.append(basic_ext_nm)
        found_images.append(full_url)
        seen_urls.add(full_url)
        
        # 최대 개수 도달 시 중단 (비용 관리)
        if len(found_images) >= max_images:
            break
    return found_images


# ==========================================
# [헬퍼 함수] 딕셔너리에서 기본+추가 이미지 모두 추출
# ==========================================
def extract_all_valid_images(img_data):
    # 데이터가 딕셔너리가 아니면(None, float 등) 빈 리스트 반환
    if not isinstance(img_data, dict):
        return []
    
    valid_urls = []

    # 1. 기본 이미지 (basicExtNm) - 필수값이어도 안전하게 get 사용
    if img_data.get('basicExtNm'):
        basicExtNm = img_data['basicExtNm']
        valid_urls.append(f"https://cdn2.halfclub.com/rimg/330x440/contain/{basicExtNm}?format=webp" )
        
    # 2. 추가 이미지 (add1ExtNm ~ add9ExtNm) 순회
    for i in range(1, 10):
        key = f"add{i}ExtNm"
        url = img_data.get(key)
        # 키가 있고, 값이 비어있지 않은 경우에만 추가
        if url:
            valid_urls.append(f"https://cdn2.halfclub.com/rimg/330x440/contain/{url}?format=webp" )
            
    return valid_urls