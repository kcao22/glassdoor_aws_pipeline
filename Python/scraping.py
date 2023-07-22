# K. Cao 6/24/23

# Imports
import configparser
import json
import time 

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# Parser to get config credentials and other private information
parser = configparser.ConfigParser()
parser.read_file(open(r'D:\Documents\Data Projects\glassdoor_aws_pipeline\credentials.config'))

# Selenium variables
selenium_driver_path = parser.get('SELENIUM', 'path')

def exit_prompt(driver):
    '''
    Avoids modal pop up by clicking away from pop up.
    
    ARGUMENTS:
        driver: Selenium driver object for scraping.
    RETURNS:
        None
    '''
    try:
        elem = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//*[@id="LoginModal"]/div/div/div[2]/button')))
        action = ActionChains(driver)
        action.move_to_element(elem).move_by_offset(250, 0).click().perform()
    except:
        pass
    
def get_value(driver, test, element, property, by_type='class'):
    '''
    Finds page element by class or by xpath.
    
    ARGUMENTS:
        driver: Selenium driver object for scraping.
        test: If test, then print Exception, else set value as None
        element: Page element.
        property: What job property scraped.
        by_type: By 'class' name or by 'xpath'.
    '''
    try:
        if by_type == 'class':
            return driver.find_element_by_class_name(element).text
        elif by_type == 'xpath':
            return driver.find_element_by_xpath(element).text
    except NoSuchElementException:
            if test:
                print(f'NoSuchElementException for {property}. Defaulting to None')
            else:
                return None

def get_all_values_dict(driver, test=False):
    '''
    Scrapes all data for each job post on a page.
    
    ARGUMENTS:
        driver: Selenium driver object for scraping.
        element: Page element.
        test: If test, then print Exception, else set value as None

    RETURNS:
        Dictionary object of all job and company properties.
    '''
    try:
        # Expand 'Show More' option for job description
        driver.find_element_by_class_name('css-t3xrds').click()
    except:
        pass
    job_info = {}
    params = {
        'CompanyName': ['css-87uc0g', 'Company Name', 'class'],
        # 'CompanyName': ['//*[@class="css-8wag7x"]', 'Company Name', 'xpath'],
        'JobTitle': ['//*[@id="JDCol"]/div/article/div/div[1]/div/div/div[1]/div[3]/div[1]/div[2]', 'Job Name', 'xpath'],
        'JobLocation': ['css-56kyx5', 'Job Location', 'class'],
        # 'JobLocation': ['//*[@class="location mt-xxsm"]', 'Job Location', 'xpath'],
        'EasyApply': ['//*[@id="MainCol"]/div[1]/ul/li[3]/div/div/a/div[1]/div[5]/div', 'Easy Apply', 'xpath'],
        'JobDescription': ['.//div[@class="jobDescriptionContent desc"]', 'Job Description', 'xpath'],
        'JobSalary': ['css-1xe2xww', 'Salary', 'class'],
        # 'JobSalary': ['//*[@class="salary-estimate"]', 'Salary', 'xpath'],
        'CompanyRating': ['css-1m5m32b', 'Company Rating', 'class'],
        # 'CompanyRating': ['//*[@class="job-search-rnnx2x"]', 'Company Rating', 'xpath'],
        'CompanySize': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[1]/span[2]', 'Company Size', 'xpath'],
        'CompanyType': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[3]/span[2]', 'Company Type', 'xpath'],
        'CompanySector': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[5]/span[2]', 'Company Sector', 'xpath'],
        'CompanyYearFounded': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[2]/span[2]', 'Year Founded', 'xpath'],
        'CompanyIndustry': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[4]/span[2]', 'Company Industry', 'xpath'],
        'CompanyRevenue': ['//div[@id="EmpBasicInfo"]/div[1]/div/div[6]/span[2]', 'Company Revenue', 'xpath']
    }

    for param in params.items():
        val = get_value(driver, test, param[1][0], param[1][1], param[1][2])
        job_info[param[0]] = job_info.get(param[0], val)
        
    return job_info

def page_count(driver, test=False):
    '''
    Uses pagination footer to determine the number of pages of job postings available.
    
    ARGUMENTS:  
        driver: Selenium driver object for scraping.
        test: If test, then print Exception, else set value as None
        
    RETURNS:
        None
    '''
    try:
        # Return the last page number. 'Page # of ##'
        return get_value(driver, test, 'paginationFooter', 'class').split(' ')[-1]
    except:
        print('NoSuchElementException')
        return None
    
def collect_all_data(test: bool, url: str = 'https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm?fromAge=1'):
    '''
    Collects data on all jobs given a starting URL for Glassdoor. Assumes iteration is from first page of URL on towards the last page.
    
    ARGUMENTS:
        test: Boolean. If test, print statements for debugging. Else catch exceptions only.
        url: Glassdoor URL to scrape data from. Must be of type string. Defaults to 'Data Engineer' jobs posted in the last 24 hours.

    RETURNS:
        all_data: All data on jobs from given Glassdoor URL
    '''
    # Set selenium driver path
    all_jobs = []
    with webdriver.Edge(r'D:\Documents\Data Projects\glassdoor_aws_pipeline\SeleniumDrivers\msedgedriver.exe') as driver:
        # Open Glasdoor page
        driver.get(url)
        driver.maximize_window()
        # Wait for page to load
        time.sleep(2)
        for p in range(int(page_count(driver, True))):
            print(f'Page Number: {p + 1}')
            # Get all jobs on page
            job_posts = driver.find_elements_by_class_name('react-job-listing')
            # Iterate through job posts
            for i, job in enumerate(job_posts):
                print(f'Job Number: {i + 1}')
                test = False
                time.sleep(1)
                job.click()
                time.sleep(2)
                exit_prompt(driver)
                try:
                    job_info = get_all_values_dict(driver, True)
                    all_jobs.append(job_info)
                    if test:  # If test, print outputs 
                        print(json.dumps(job_info))
                        print()
                except Exception as e:
                    print(e)
                    time.sleep(4)
                if job == job_posts[-1]:
                    print('Last Job')
            page_element = f'//*[@id="MainCol"]/div[2]/div/div[1]/button[{p + 3}]'
            try:
                driver.find_element_by_xpath(page_element).click()
                exit_prompt(driver)
                time.sleep(2)
            except NoSuchElementException:
                print('No Page Element Found')
                break
    return all_jobs

