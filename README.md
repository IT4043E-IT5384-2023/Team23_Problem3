<!-- PROJECT LOGO -->
<div align="center">

  <h1 align="center"><br>Twitter post by topic</br></h1>

</div>


<!-- TABLE OF CONTENTS
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-repository">About The Repository</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#usage">Usage</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>

  </ol>
</details>

 -->

<!-- ABOUT THE REPOSITORY -->
## About The Repository

This repository is used to build data crawlers for Twitter post by topic.


<!-- 
### Built With
[![Python][Python.com]][Python-url]
 -->


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

Install all packages mentioned in requirements.txt
   ```sh
   pip install -r requirements.txt
   ```

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/your_username_/Repository-Name.git
   ```
2. Install all packages mentioned in requirements.txt
   ```sh
   pip install -r requirements.txt
   ```
3. Create a .env file and configure all of the following environment variables.
- TWITTER_USER_NAME: Username for your accounts in twitter (Ex. 'username1, username2, username3...')
- TWITTER_USER_PASSWORD:  (Your password)

Note: 
  - All accounts must have save password
  - Number of Threads in config.py should be approximately equal to number of accounts

### Usage

- Run command in cmd
   ```sh
   python main.py
   ```

<!-- MARKDOWN LINKS & IMAGES
[Python.com]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/ -->