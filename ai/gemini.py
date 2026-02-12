import json
import base64
from google import genai
from google.genai import types
from schema.product import ProductSchema # 사용자가 정의한 스키마

# --- [내부 함수 1] Google Gemini 호출 로직 ---
def _call_gemini_api(system_prompt, user_text, image_list, model_name, api_key):
     # API 키 설정 (환경변수나 별도 설정 파일에서 가져오는 것을 권장)
    # st.secrets["GOOGLE_API_KEY"] 등을 사용할 수 있습니다.        
    client = genai.Client(api_key=api_key)

    # 1. 안전 설정 (Safety Settings) - 리스트 형태로 변경 및 열거형 타입 적용
    # 패션 이커머스 이미지는 성인용 콘텐츠로 오인받기 쉬우므로 BLOCK_NONE 설정을 정확히 주입해야 합니다.
    safety_settings = [
        types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
        types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
        types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
        types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
    ]

    # 2. Generation Config
    generation_config = types.GenerateContentConfig(
        temperature=0.1,
        response_mime_type="application/json",
        response_schema=ProductSchema,
        system_instruction=system_prompt,
        safety_settings=safety_settings
    )

    # 3. 멀티모달 데이터 구성
    content_parts = [user_text]
    for img_data in image_list:
        if isinstance(img_data, str) and "base64," in img_data:
            header, encoded = img_data.split("base64,", 1)
            mime_type = header.split(":")[1].split(";")[0]
            img_bytes = base64.b64decode(encoded)
            # types.Part.from_bytes 사용 권장
            content_parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    # 4. 첫 번째 시도 (이미지 포함)
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=content_parts,
            config=generation_config
        )
    except Exception as e:
        print(f"API 호출 에러: {e}")
        response = None

    # 5. 차단 확인 및 재시도 로직 (Fallback)
    # response.text 접근 시 차단된 경우 에러가 발생할 수 있어 안전하게 체크합니다.
    is_blocked = False
    try:
        if not response or not response.text:
            is_blocked = True
    except:
        is_blocked = True

    if is_blocked:
        reason = "Unknown"
        if response and response.prompt_feedback:
            reason = response.prompt_feedback.block_reason
        
        if len(image_list) > 0:
            print(f"⚠️ 이미지 분석 차단됨 (사유: {reason}). 텍스트 모드로 재시도합니다.")
            print("⚠️ 이미지 보안 정책으로 인해 텍스트만 분석합니다.")
            
            # 텍스트 모드 재시도 시에도 config를 동일하게 전달하여 JSON 형식을 유지합니다.
            response = client.models.generate_content(
                model=model_name,
                contents=[user_text],
                config=generation_config
            )
        else:
            print(f"❌ Gemini가 응답을 거부했습니다. (사유: {reason})")
            return None

    # 6. 최종 응답 반환 (parsed 기능 활용)
    try:
        return response.parsed
    except Exception:
        return ProductSchema(**json.loads(response.text))