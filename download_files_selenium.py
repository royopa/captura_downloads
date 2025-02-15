from splinter import Browser


def main():
    browser = Browser('edge', incognito=True)
    browser.visit('https://www.google.com')


if __name__ == '__main__':
    main()
