from setuptools import setup, find_packages

setup(
    name="stream-data-producer",
    version="0.1.0",
    description="A configurable single-stream data generator for testing and simulation",
    author="Shi Mengzhao",
    author_email="shimengzhao@example.com",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pyyaml>=6.0",
        "confluent-kafka>=2.0.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.20.0",
        "click>=8.0.0",
        "psutil>=5.9.0",
        "requests>=2.28.0",
        "httpx>=0.23.0",
    ],
    extras_require={
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "stream-data-producer=stream_data_producer.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Testing",
        "Topic :: System :: Distributed Computing",
    ],
    url="https://github.com/yourusername/stream-data-producer",
)