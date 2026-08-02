"""위키피디아에서 S&P 500 티커를 확인하는 스크립트."""

import pandas as pd
from datetime import datetime
from io import BytesIO
import urllib.request


class SP500UniverseError(RuntimeError):
    """S&P 500 유니버스를 신뢰할 수 있게 확보하지 못했다.

    네트워크·파싱 실패 시 축약된 대체 목록을 조용히 돌려주면 호출자가
    그 목록을 정본 티커 파일에 그대로 덮어써서 유니버스가 영구히 손실된다.
    따라서 실패는 값이 아니라 예외로 전달한다.
    """


def get_historical_sp500_tickers(years=10):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # 403 Forbidden 우회를 위해 브라우저 정보 설정
    req = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    )

    try:
        with urllib.request.urlopen(req) as response:
            html_content = response.read()
            # pandas 3.x에서는 bytes를 HTML 내용이 아닌 파일 경로로 해석하므로
            # 파일형 객체로 감싸서 전달한다.
            tables = pd.read_html(BytesIO(html_content))
    except Exception as exc:
        raise SP500UniverseError(
            f"위키피디아에서 S&P 500 구성 종목을 가져오지 못했습니다: {exc}"
        ) from exc

    if len(tables) < 2:
        raise SP500UniverseError(
            "위키피디아 문서 구조가 예상과 다릅니다: 구성 종목/변경 이력 표를 찾지 못했습니다."
        )

    try:
        return _parse_sp500_tables(tables, years)
    except SP500UniverseError:
        raise
    except Exception as exc:
        raise SP500UniverseError(
            f"위키피디아 S&P 500 표를 해석하지 못했습니다: {exc}"
        ) from exc


def _parse_sp500_tables(tables, years):
    # 1. 첫 번째 테이블: 현재 구성 종목
    df_current = tables[0]
    current_tickers = set(df_current['Symbol'].str.replace('.', '/', regex=False).tolist())
    
    # 2. 두 번째 테이블: 구성 종목 변경 이력
    df_changes = tables[1].copy()
    
    # MultiIndex의 상위 분류(Added/Removed)를 보존해서 단일 레벨로 전환한다.
    # 마지막 레벨만 사용하면 두 컬럼이 모두 "Ticker"가 되어 pandas 3.x의
    # 중복 컬럼 처리에서 오류가 발생한다.
    if isinstance(df_changes.columns, pd.MultiIndex):
        flattened_columns = []
        for column in df_changes.columns:
            parts = []
            for part in column:
                part = str(part).strip()
                if part and not part.startswith('Unnamed:') and part not in parts:
                    parts.append(part)
            flattened_columns.append(' '.join(parts))
        df_changes.columns = flattened_columns
    else:
        df_changes.columns = [str(col).strip() for col in df_changes.columns]
        
    # 'Date' 컬럼 확보 및 날짜 변환
    if 'Date' not in df_changes.columns:
        df_changes.rename(columns={df_changes.columns[0]: 'Date'}, inplace=True)
    
    df_changes['Date'] = pd.to_datetime(df_changes['Date'], errors='coerce')
    df_changes = df_changes.dropna(subset=['Date'])
    
    # 요청한 연수만큼의 S&P 500 편출 이력을 포함한다.
    history_cutoff = pd.Timestamp(datetime.now()) - pd.DateOffset(years=years)
    recent_changes = df_changes[df_changes['Date'] >= history_cutoff]
    
    # 편출(Removed)된 종목들 확보
    removed_tickers = []
    
    # 'Removed' 관련 열들을 모두 수색
    removed_cols = [col for col in recent_changes.columns if 'Removed' in col and 'Ticker' in col]
    
    for col in removed_cols:
        col_series = recent_changes[col]

        # 문자열 정제 및 슬래시 포맷 변환
        tickers_to_add = col_series.dropna().astype(str).str.replace('.', '/', regex=False).tolist()
        # 노이즈 정제 (공백 제거 및 'Removed' 등 본문 텍스트 제외)
        tickers_to_add = [t.strip() for t in tickers_to_add if t.strip() and len(t.strip()) <= 6 and not any(char.isdigit() for char in t)]
        removed_tickers.extend(tickers_to_add)
            
    # 3. 현재 종목과 지정 기간 동안 퇴출된 종목 병합
    all_historical_tickers = current_tickers.union(set(removed_tickers))
    
    # 대문자 표준화 및 최종 길이 필터링
    final_tickers = {t.upper() for t in all_historical_tickers if t and len(t) <= 6 and (t.isalpha() or '/' in t)}

    if not final_tickers:
        raise SP500UniverseError(
            "위키피디아 표에서 유효한 S&P 500 티커를 하나도 얻지 못했습니다."
        )

    return sorted(list(final_tickers))

if __name__ == "__main__":
    print("위키피디아에서 S&P 500 히스토리컬 티커 수집 중...")
    tickers = get_historical_sp500_tickers(years=10)
    print(f"\n최근 10년간 S&P 500에 존재했던 총 티커 수: {len(tickers)}")
    print("티커 예시 (앞 20개):", tickers[:20])
