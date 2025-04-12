
import setuptools


setuptools.setup(
    name="papaya-wrapper", # Replace with your desired package name on PyPI if publishing
    version="0.1.0",      # Start with a version number
    author="Your Name",    # Replace with your name
    author_email="your.email@example.com", # Replace with your email
    description="A wrapper to monkey-patch FastAPI for in-memory logging via uvicorn.",
    long_description="",
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/papaya-project", # Replace with your project URL if available
    packages=setuptools.find_packages(), # Automatically finds the 'papaya' package
    install_requires=[
        # List runtime dependencies
        # Versions are examples, adjust as needed
        'fastapi>=0.70.0',
        'uvicorn[standard]>=0.15.0', # Include [standard] for common extras like websockets, http-tools
    ],
    python_requires='>=3.7', # Specify minimum Python version compatibility
    classifiers=[
        # Trove classifiers
        # Get more from https://pypi.org/classifiers/
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License", # Choose your license
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",      # Or Beta, Production/Stable
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: System :: Logging",
    ],
    entry_points={
        # This creates the 'papaya' command in the user's PATH
        'console_scripts': [
            'papaya=papaya.run_papaya:main',
        ],
    },
)
