#!/usr/bin/env python3
"""
Test script for AI integration in DataPro Agent
This script tests the AI chat functionality and suggestions
"""

import requests
import json
import time

# Backend URL
BASE_URL = "http://localhost:8000"

def test_ai_chat():
    """Test the AI chat endpoint"""
    print("🤖 Testing AI Chat Integration...")
    
    # Test basic chat
    chat_data = {
        "message": "Hello, I need help with data cleaning",
        "context": {
            "file_id": None,
            "current_page": "upload",
            "data_loaded": False
        }
    }
    
    try:
        response = requests.post(f"{BASE_URL}/api/chat", json=chat_data)
        if response.status_code == 200:
            data = response.json()
            print("✅ AI Chat Response:")
            print(f"Response: {data.get('response', 'No response')}")
            print(f"Suggestions: {len(data.get('suggestions', []))} suggestions")
            print()
        else:
            print(f"❌ Chat failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Chat error: {e}")

def test_ai_suggestions():
    """Test AI suggestions endpoint"""
    print("🎯 Testing AI Suggestions...")
    
    # Test suggestions without file
    suggestions_data = {
        "file_id": "test-file-id",
        "type": "general"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/api/ai/suggestions", json=suggestions_data)
        if response.status_code == 200:
            data = response.json()
            print("✅ AI Suggestions Response:")
            print(f"Suggestions: {len(data.get('suggestions', []))} suggestions")
            if data.get('suggestions'):
                for suggestion in data['suggestions'][:3]:  # Show first 3
                    print(f"  - {suggestion.get('action', 'Unknown')}: {suggestion.get('description', 'No description')}")
            print()
        else:
            print(f"❌ Suggestions failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Suggestions error: {e}")

def test_backend_health():
    """Test if backend is running"""
    print("🏥 Testing Backend Health...")
    
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print("✅ Backend is running")
            return True
        else:
            print(f"❌ Backend health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Backend not accessible: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 DataPro AI Integration Test")
    print("=" * 50)
    
    # Check if backend is running
    if not test_backend_health():
        print("\n❌ Backend is not running. Please start it with: python backend.py")
        return
    
    print()
    
    # Test AI chat
    test_ai_chat()
    
    # Test AI suggestions
    test_ai_suggestions()
    
    print("🎉 AI Integration Test Complete!")
    print("\nTo test with real data:")
    print("1. Upload a dataset through the frontend")
    print("2. Open the AI chat (💬 button)")
    print("3. Ask questions like 'Analyze my data' or 'Help with cleaning'")
    print("4. Click '🎯 Smart Suggestions' for AI-powered recommendations")

if __name__ == "__main__":
    main()
