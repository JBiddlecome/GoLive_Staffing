# Developer & LLM Guidelines for Test Scripts

> **Notice to all LLMs and AI Agents:** 

All standalone test scripts, debugging scripts, and temporary data exploration tools used for this repository must be placed in the `Test_Scripts` folder. 

1. **Do not create new test scripts in the root directory.** 
2. Any generated files like `test_*.py`, `tmp_*.py`, `error_debug.txt`, or other diagnostic text files should go directly into the `Test_Scripts` subdirectory to keep the root directory perfectly clean and organized for the production application structure.
3. If an existing test file is required for debugging or schema investigation, please look for it in `Test_Scripts/`. 

Thank you for helping maintain a clean workspace!
