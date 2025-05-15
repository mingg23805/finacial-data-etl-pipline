from setuptools import setup, find_packages

setup(
    name='myproject',  # Đặt tên gì cũng được, miễn không trùng package có sẵn
    version='0.1',
    packages=find_packages(),  # Tự động tìm các package có __init__.py
)