import subprocess
import sys
import os

def verify_notebook():
    notebook_path = "retail_analytics.ipynb"
    output_path = "retail_analytics_verified.ipynb"
    
    print(f"Verifying {notebook_path}...")
    
    if not os.path.exists(notebook_path):
        print(f"Error: {notebook_path} not found.")
        sys.exit(1)

    try:
        # Execute the notebook using nbconvert
        # We use sys.executable to ensure we use the current python environment (venv)
        cmd = [
            sys.executable, "-m", "jupyter", "nbconvert", 
            "--to", "notebook", 
            "--execute", notebook_path, 
            "--output", output_path
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        
        print(f"\n✅ Verification Successful: {notebook_path} ran without errors.")
        
        # Clean up the output file to keep directory clean
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Cleaned up {output_path}")
            
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Verification Failed: Notebook execution encountered errors.")
        sys.exit(1)

if __name__ == "__main__":
    verify_notebook()
