import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# 设置Chrome选项
chrome_options = Options()
# 去掉无头模式，便于观察
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36')

try:
    print("启动Chrome浏览器...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    print("访问目标网站...")
    driver.get("https://www.ticainiu.com/channel/xueqiu.html")
    
    # 等待页面加载
    print("等待页面加载...")
    time.sleep(5)
    
    # 保存页面源码
    with open('test_page.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)
    print("已保存页面源码到test_page.html")
    
    # 打印页面标题
    print(f"页面标题: {driver.title}")
    
    # 尝试查找页面上的一些基本元素
    print("尝试查找页面元素...")
    links = driver.find_elements("tag name", "a")
    print(f"找到 {len(links)} 个链接")
    
    # 保存截图
    driver.save_screenshot("page_screenshot.png")
    print("已保存页面截图")
    
except Exception as e:
    print(f"测试过程中出错: {e}")
finally:
    try:
        driver.quit()
        print("浏览器已关闭")
    except:
        pass 