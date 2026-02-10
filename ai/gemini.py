import json
import base64
import google.generativeai as genai  # Gemini용 SDK
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from schema.product import ProductSchema # 사용자가 정의한 스키마

# --- [내부 함수 1] Google Gemini 호출 로직 ---
def _call_gemini_api(system_prompt, user_text, image_list, model_name, api_key):
    try:
        # API 키 설정 (환경변수나 별도 설정 파일에서 가져오는 것을 권장)
        # st.secrets["GOOGLE_API_KEY"] 등을 사용할 수 있습니다.
        genai.configure(api_key=api_key) 
        
        # Generation Config (JSON 응답 강제)
        generation_config = {
            "temperature": 0.2,
            "response_mime_type": "application/json",
            "response_schema": ProductSchema # Pydantic 모델 직접 지원
        }

        # ★ [수정] 안전 설정 추가 (속옷/수영복 등 패션 상품 차단 방지)
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }

        model = genai.GenerativeModel(
            model_name=model_name, # 예: "gemini-2.5-flash"
            system_instruction=system_prompt,
            generation_config=generation_config
        )

        # 1. 멀티모달(텍스트+이미지) 구성
        content_parts = [user_text]
        for img_data in image_list:
            if isinstance(img_data, str) and "base64," in img_data:
                header, encoded = img_data.split("base64,", 1)
                mime_type = header.split(":")[1].split(";")[0]
                img_bytes = base64.b64decode(encoded)
                content_parts.append({"mime_type": mime_type, "data": img_bytes})

        # 2. 첫 번째 시도 (이미지 포함)
        try:
            # 안전 설정 적용하여 호출
            response = model.generate_content(content_parts, safety_settings=safety_settings)

        except Exception:
            response = None

        # 3. 차단 확인 및 재시도 로직 (Fallback)
        # 응답이 없거나, parts가 비어있으면 차단된 것임
        if not response or not response.candidates:
            reason = "Unknown"
            if response and response.prompt_feedback:
                reason = response.prompt_feedback.block_reason
            
            # 이미지가 포함되어 있었고 차단되었다면 -> 텍스트로만 재시도
            if len(image_list) > 0:
                print(f"⚠️ 이미지 분석 차단됨 (사유: {reason}). 텍스트 모드로 재시도합니다.")
                print(f"⚠️ 이미지 보안 정책으로 인해 텍스트만 분석합니다.")
                
                # 이미지를 뺀 순수 텍스트만 전송
                response = model.generate_content([user_text])
            else:
                print(f"❌ Gemini가 응답을 거부했습니다. (사유: {reason})")
                return None
            

        return ProductSchema(**json.loads(response.text))

    except Exception as e:
        print(f"Gemini 호출 오류: {e}")
        return None