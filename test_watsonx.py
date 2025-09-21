python -c "
import os
from dotenv import load_dotenv
load_dotenv()

# Quick env check
required = ['WX_API_KEY', 'WX_PROJECT_ID', 'WX_URL']
missing = [v for v in required if not os.getenv(v)]
if missing:
    print(f'❌ Missing: {missing}')
    exit(1)

# Test the client
import sys
sys.path.append('app/llm')
from watsonx_client import llm_generate

print('🧪 Testing watsonx...')
response = llm_generate('What is BHA? Answer in one sentence.')
print('✅ Success!' if response else '❌ Failed')
print(f'Response: {response[:100]}...' if response else 'No response')
"