import asyncio
import openai

async def f():
    try:
        client = openai.AsyncOpenAI(api_key="")
        print("Got client")
        response = await client.chat.completions.create(
            model='gpt-4o-mini', 
            messages=[{'role': 'user', 'content': 'hi'}]
        )
        print("Success!", response)
    except Exception as e:
        print("Error:", repr(e))

asyncio.run(f())
