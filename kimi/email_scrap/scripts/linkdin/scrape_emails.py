import asyncio
import json
import pandas as pd
from crawl4ai import AsyncWebCrawler
from openai import OpenAI
import re
from urllib.parse import urlparse
import random
from typing import Dict, List
import sys
import io
import contextlib

# --- CONFIGURATION ---
INPUT_CSV = 'apify_results_merged_20251205_135421.csv'
OUTPUT_CSV = 'linkedin_leads_with_emails_gemma2.csv'
OLLAMA_URL = "http://localhost:11434/v1"  # Ollama OpenAI-compatible API
MODEL_NAME = "gemma2:2b"  # Ollama model name

# Crawler settings
CRAWLER_TIMEOUT = 30  # seconds
MAX_RETRIES = 2
RETRY_DELAY = 2  # seconds
MIN_DELAY_BETWEEN_REQUESTS = 0.5  # seconds
MAX_DELAY_BETWEEN_REQUESTS = 2.0  # seconds

# Important columns to keep in output
OUTPUT_COLUMNS = ['headquarter', 'industry', 'name', 'tagline', 'url', 'urn', 'websiteUrl', 'scraped_email']

# Initialize OpenAI client pointing to Ollama (OpenAI-compatible API)
client = OpenAI(base_url=OLLAMA_URL, api_key="ollama", timeout=45)

# Profile rotation system
class ProfileRotator:
    """Manages rotation through multiple browser profiles to avoid blocking"""
    
    def __init__(self):
        self.profiles = self._generate_profiles()
        self.current_index = 0
        self.lock = asyncio.Lock()
    
    def _generate_profiles(self) -> List[Dict]:
        """Generate 50+ unique browser profiles with different fingerprints"""
        profiles = []
        
        # User agents for different browsers and OS combinations
        user_agents = [
            # Chrome on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            
            # Chrome on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            
            # Firefox on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            
            # Firefox on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1; rv:121.0) Gecko/20100101 Firefox/121.0",
            
            # Safari on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            
            # Edge on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
            "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",
            
            # Chrome on Linux
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            
            # Firefox on Linux
            "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
            
            # Chrome on Android
            "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 13; SM-A515F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 11; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
            
            # Safari on iOS
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            
            # Additional Chrome variations
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        
        # Viewport sizes (width, height)
        viewports = [
            (1920, 1080), (1366, 768), (1536, 864), (1440, 900), (1280, 720),
            (1600, 900), (2560, 1440), (1920, 1200), (1680, 1050), (1280, 1024),
            (1360, 768), (1024, 768), (1280, 800), (1440, 1080), (1600, 1200),
            (3840, 2160), (2560, 1600), (1920, 1080), (1680, 945), (1536, 864),
            (1280, 720), (1024, 600), (800, 600), (1920, 1440), (1280, 960),
        ]
        
        # Languages
        languages = [
            "en-US,en", "en-GB,en", "en-CA,en", "en-AU,en", "en-NZ,en",
            "es-ES,es", "es-MX,es", "es-AR,es", "fr-FR,fr", "fr-CA,fr",
            "de-DE,de", "it-IT,it", "pt-BR,pt", "pt-PT,pt", "nl-NL,nl",
            "pl-PL,pl", "ru-RU,ru", "ja-JP,ja", "ko-KR,ko", "zh-CN,zh",
            "zh-TW,zh", "ar-SA,ar", "hi-IN,hi", "tr-TR,tr", "sv-SE,sv",
        ]
        
        # Timezones
        timezones = [
            "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
            "America/Toronto", "America/Vancouver", "Europe/London", "Europe/Paris",
            "Europe/Berlin", "Europe/Rome", "Europe/Madrid", "Europe/Amsterdam",
            "Europe/Stockholm", "Europe/Warsaw", "Europe/Moscow", "Asia/Tokyo",
            "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Singapore", "Asia/Dubai",
            "Asia/Kolkata", "Australia/Sydney", "Australia/Melbourne", "Pacific/Auckland",
        ]
        
        # Generate 50+ unique profiles
        for i in range(60):  # Generate 60 profiles
            profile = {
                'user_agent': random.choice(user_agents),
                'viewport': random.choice(viewports),
                'language': random.choice(languages),
                'timezone': random.choice(timezones),
                'platform': self._extract_platform(random.choice(user_agents)),
            }
            profiles.append(profile)
        
        # Shuffle to randomize order
        random.shuffle(profiles)
        return profiles
    
    def _extract_platform(self, user_agent: str) -> str:
        """Extract platform from user agent"""
        if 'Windows' in user_agent:
            return 'Win32'
        elif 'Macintosh' in user_agent:
            return 'MacIntel'
        elif 'Linux' in user_agent:
            return 'Linux x86_64'
        elif 'Android' in user_agent:
            return 'Linux armv8l'
        elif 'iPhone' in user_agent or 'iPad' in user_agent:
            return 'iPhone'
        return 'Win32'
    
    async def get_profile(self) -> Dict:
        """Get next profile in rotation (thread-safe)"""
        async with self.lock:
            profile = self.profiles[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.profiles)
            return profile
    
    def get_random_profile(self) -> Dict:
        """Get a random profile (for initial setup)"""
        return random.choice(self.profiles)

# Initialize profile rotator
profile_rotator = ProfileRotator()

async def get_email_from_ai(text, url):
    """
    Sends cleaned Markdown to Ollama to extract the email.
    """
    # 1. Truncate text to fit context window (8000 chars is usually enough for contact pages)
    # We prioritize the footer and header where emails usually hide.
    content = text[:4000] + "\n...[skipped]...\n" + text[-4000:]
    
    prompt = f"""
    You are a data extraction engine.
    Analyze the text from the website: {url}
    Find the primary contact email address.
    
    Rules:
    - Return strictly a JSON object: {{"email": "found_email@domain.com"}}
    - If multiple found, prefer 'sales', 'info', 'support', or 'contact'.
    - If the email is obfuscated (e.g., 'sales at domain dot com'), fix it.
    - If NO email is found, return {{"email": null}}.
    
    Website Text:
    {content}
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a data extraction engine. Always respond with valid JSON only, no other text."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        # Parse JSON from response (Ollama returns text, we extract JSON)
        content = response.choices[0].message.content.strip()
        
        # Try to extract JSON if it's wrapped in markdown code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        result = json.loads(content)
        return result.get("email")
    except json.JSONDecodeError as e:
        print(f"  [!] AI JSON parse error on {url}: {e}")
        # Try to extract email directly from response text as fallback
        try:
            email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', content)
            if email_match:
                return email_match.group(0)
        except:
            pass
        return None
    except Exception as e:
        print(f"  [!] AI Error on {url}: {e}")
        return None

def normalize_url(url):
    """Normalize URL by adding protocol if missing"""
    if pd.isna(url):
        return None
    
    url_str = str(url).strip()
    if not url_str:
        return None
    
    # Add https:// if protocol is missing
    if not url_str.startswith(('http://', 'https://')):
        url_str = 'https://' + url_str
    
    return url_str

def is_valid_url(url):
    """Validate URL format and accessibility"""
    if pd.isna(url):
        return False
    
    url_str = str(url).strip()
    if not url_str:
        return False
    
    # Normalize URL (add protocol if missing)
    url_str = normalize_url(url_str)
    if not url_str:
        return False
    
    try:
        parsed = urlparse(url_str)
        if not parsed.netloc:
            return False
        return True
    except Exception:
        return False

async def crawl_with_retry(crawler, url, max_retries=MAX_RETRIES):
    """Crawl URL with retry logic, timeout handling, and profile rotation"""
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                print(f"  [↻] Retry {attempt}/{max_retries} after {wait_time}s...")
                await asyncio.sleep(wait_time)
            
            # Get a profile for this request (rotates through profiles)
            profile = await profile_rotator.get_profile()
            
            # Prepare headers with profile information
            headers = {
                'User-Agent': profile['user_agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': profile['language'],
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
            
            # Use asyncio.wait_for to enforce timeout
            result = await asyncio.wait_for(
                crawler.arun(
                    url=url,
                    headers=headers,
                    viewport_width=profile['viewport'][0],
                    viewport_height=profile['viewport'][1],
                ),
                timeout=CRAWLER_TIMEOUT
            )
            
            if result.success:
                return result, None
            else:
                last_error = f"Crawler returned success=False"
                if hasattr(result, 'error'):
                    last_error = result.error
                
        except asyncio.TimeoutError:
            last_error = f"Timeout after {CRAWLER_TIMEOUT}s"
            if attempt < max_retries:
                continue
        except ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            if attempt < max_retries:
                continue
        except Exception as e:
            error_type = type(e).__name__
            last_error = f"{error_type}: {str(e)}"
            # Don't retry on certain errors
            if any(x in str(e).lower() for x in ['404', '403', '401', 'ssl', 'certificate']):
                break
            if attempt < max_retries:
                continue
    
    return None, last_error

async def process_row(crawler, row):
    url = row['websiteUrl']
    company = row['name']
    
    # Validate and normalize URL
    if not is_valid_url(url):
        print(f"  [✗] Invalid URL: {company} ({url})")
        return None
    
    # Normalize URL (add https:// if missing)
    url = normalize_url(url)
    
    # Random delay between requests to avoid detection
    delay = random.uniform(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
    await asyncio.sleep(delay)

    print(f"Processing: {company} ({url})")
    
    try:
        # 1. Crawl the Homepage with retry logic
        result, error = await crawl_with_retry(crawler, url)
        
        if result is None:
            print(f"  [✗] Failed to access {url} - {error}")
            return None
            
        # 2. Check Homepage Text First (Fastest)
        # Simple regex check before calling AI to save time
        # (This handles simple cases like mailto: links instantly)
        simple_emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', result.markdown)
        if simple_emails:
            # filter junk emails
            valid_emails = [e for e in simple_emails if not any(x in e for x in ['example', 'wix', 'sentry', 'placeholder', 'test'])]
            if valid_emails:
                print(f"  [+] Found via Regex: {valid_emails[0]}")
                return valid_emails[0]

        # 3. If not in homepage, look for Contact Page
        # Crawl4AI extracts links automatically
        contact_link = None
        if result.links:
            internal_links = result.links.get('internal', [])
            for link in internal_links:
                href = link.get('href', '')
                txt = link.get('text', '').lower()
                if 'contact' in txt or 'about' in txt or 'contact' in href:
                    # Make sure it's a full URL
                    if href.startswith('http'):
                        contact_link = href
                    elif href.startswith('/'):
                        parsed = urlparse(url)
                        contact_link = f"{parsed.scheme}://{parsed.netloc}{href}"
                    break
        
        target_url = contact_link if contact_link else url
        
        # 4. Crawl the Target Page (if different) with retry logic
        if target_url != url:
            print(f"  [->] Following link: {target_url}")
            contact_result, contact_error = await crawl_with_retry(crawler, target_url)
            if contact_result is None:
                print(f"  [✗] Failed to access contact page: {contact_error}")
                # Continue with homepage result
            else:
                result = contact_result
        
        # 5. Ask AI to Extract from the Markdown
        email = await get_email_from_ai(result.markdown, url)
        if email:
            print(f"  [+] Found via AI: {email}")
        else:
            print(f"  [-] No email found.")
            
        return email

    except asyncio.TimeoutError:
        print(f"  [✗] Timeout error for {url}")
        return None
    except ConnectionError as e:
        print(f"  [✗] Connection error for {url}: {e}")
        return None
    except Exception as e:
        error_type = type(e).__name__
        print(f"  [✗] {error_type} for {url}: {e}")
        return None

async def main():
    # 1. Load Data
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} rows from {INPUT_CSV}")
    
    # Show profile rotation info
    print(f"\n{'='*60}")
    print(f"Profile Rotation System Active")
    print(f"Total profiles available: {len(profile_rotator.profiles)}")
    print(f"Profiles will rotate automatically to avoid blocking")
    print(f"Delay between requests: {MIN_DELAY_BETWEEN_REQUESTS}-{MAX_DELAY_BETWEEN_REQUESTS}s")
    print(f"{'='*60}\n")
    
    # Verify required columns exist
    required_cols = ['name', 'websiteUrl']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Create output column
    df['scraped_email'] = None
    
    # Ensure all output columns exist (fill missing with None)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns and col != 'scraped_email':
            df[col] = None
            print(f"  [⚠] Warning: Column '{col}' not found in input, will be empty in output")
    
    # 2. Initialize Crawler with timeout settings
    async with AsyncWebCrawler(
        verbose=False,
        headless=True,
        browser_type="chromium"
    ) as crawler:
        # Process row by row
        total_rows = len(df)
        for index, row in df.iterrows():
            try:
                email = await process_row(crawler, row)
                df.at[index, 'scraped_email'] = email
                
                # Save progress every 10 rows
                if (index + 1) % 10 == 0:
                    # Filter to only important columns and only rows with emails
                    output_df = df[OUTPUT_COLUMNS].copy()
                    # Only keep rows where scraped_email is not null/empty
                    output_df = output_df[output_df['scraped_email'].notna() & (output_df['scraped_email'] != '')]
                    output_df.to_csv(OUTPUT_CSV, index=False)
                    emails_so_far = len(output_df)
                    print(f"  [💾] Progress saved: {index + 1}/{total_rows} rows processed, {emails_so_far} with emails")
                    
            except KeyboardInterrupt:
                print("\n[!] Interrupted by user. Saving progress...")
                # Save only rows with emails before breaking
                output_df = df[OUTPUT_COLUMNS].copy()
                output_df = output_df[output_df['scraped_email'].notna() & (output_df['scraped_email'] != '')]
                output_df.to_csv(OUTPUT_CSV, index=False)
                print(f"  [💾] Saved {len(output_df)} companies with emails")
                break
            except Exception as e:
                print(f"  [✗] Unexpected error processing row {index}: {e}")
                continue

    # Final Save - filter to only important columns and only rows with emails
    output_df = df[OUTPUT_COLUMNS].copy()
    # Only keep rows where scraped_email is not null/empty
    output_df = output_df[output_df['scraped_email'].notna() & (output_df['scraped_email'] != '')]
    output_df.to_csv(OUTPUT_CSV, index=False)
    
    # Print summary
    total_processed = len(df)
    emails_found = len(output_df)
    profiles_used = profile_rotator.current_index
    print(f"\n{'='*60}")
    print(f"Done! Results saved to {OUTPUT_CSV}")
    print(f"Total rows processed: {total_processed}")
    print(f"Companies with emails: {emails_found} ({emails_found/total_processed*100:.1f}%)")
    print(f"Companies without emails (excluded): {total_processed - emails_found}")
    print(f"Profiles rotated: {profiles_used} unique profiles used")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
