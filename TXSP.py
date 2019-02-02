# -*- coding: utf-8 -*-
# 此程序用来抓取腾讯视频的数据
import hashlib
import os

import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		self.limit_count = 50000000
		# self.date = '2000-01-01'
		self.db = database()
	
	def get_headers(self):
		# user_agent = self.ua.chrome
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
					   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
					   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
					   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
					   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
					   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
					   'Opera/9.52 (Windows NT 5.0; U; en)',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
					   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
					   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'host': "video.coral.qq.com",
		           'connection': "keep-alive",
		           'user-agent': user_agent,
		           'accept': "*/*",
		           'referer': "http://v.qq.com/txyp/coralComment_yp_1.0.htm",
		           'accept-encoding': "gzip, deflate",
		           'accept-language': "zh-CN,zh;q=0.9"
		           }
		return headers
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		x = re.sub(re.compile('<.*?>',re.S),'',x)
		x = re.sub(re.compile('\n'), ' ', x)
		x = re.sub(re.compile('\r'), ' ', x)
		x = re.sub(re.compile('\r\n'), ' ', x)
		x = re.sub(re.compile('[\r\n]'), ' ', x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		return x.strip()

	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HK847SP62Z59N54D"
		proxyPass = "C0604DD40C0DD358"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_comment_short(self, film_url, film_id, product_number, plat_number):
		'''
		获取某个产品的腾讯视频的短评
		:param film_id: 产品网页id
		:param product_number: 产品数据库id
		:param plat_number: 平台id
		:return: 产品的短评
		'''
		print film_id
		url = 'http://coral.qq.com/article/%s/comment/v2' % film_id
		commentid = '0'
		querystring = {"orinum": "10", "oriorder": "o", "pageflag": "1", "cursor": commentid,
		               "scorecursor": "0", "orirepnum": "2", "reporder": "o", "reppageflag": "1", "source": "1"}
		retry = 5
		results = []
		last_modify_date = self.p_time(time.time())
		limit_page = self.limit_count / 10
		page = 1
		while 1:
			try:
				print 'page:',page
				if page > limit_page:
					return results
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(), timeout=10,
				                    params=querystring).json()['data']
				last_id = text['last']
				hasnext = text['hasnext']
				names = text['userList']
				for item in text['oriCommList']:
					try:
						nick_name = names[item['userid']]['nick']
					except:
						nick_name = ''
					try:
						tmp1 = self.p_time(item['time'])
						cmt_date = tmp1.split()[0]
						# if cmt_date < self.date:
						# 	continue
						cmt_time = tmp1
					except:
						cmt_date = ''
						cmt_time = ''
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['up'])
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['orireplynum'])
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					# last_modify_date = self.p_time(time.time())
					source_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				break
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
		if not hasnext:
			return results
		else:
			while hasnext:
				page += 1
				print 'page:',page
				if page > limit_page:
					return results
				querystring = {"orinum": "10", "oriorder": "o", "pageflag": "1", "cursor": last_id,
				               "scorecursor": "0", "orirepnum": "2", "reporder": "o", "reppageflag": "1", "source": "1"}
				retry = 5
				while 1:
					try:
						text = requests.get(url, headers=self.get_headers(), timeout=10, proxies=self.GetProxies(),
						                    params=querystring).json()[
							'data']
						last_id = text['last']
						hasnext = text['hasnext']
						names = text['userList']
						for item in text['oriCommList']:
							try:
								nick_name = names[item['userid']]['nick']
							except:
								nick_name = ''
							try:
								tmp1 = self.p_time(item['time'])
								cmt_date = tmp1.split()[0]
								# if cmt_date < self.date:
								# 	continue
								cmt_time = tmp1
							except:
								cmt_date = ''
								cmt_time = ''
							try:
								comments = self.replace(item['content'])
							except:
								comments = ''
							try:
								like_cnt = str(item['up'])
							except:
								like_cnt = '0'
							try:
								cmt_reply_cnt = str(item['orireplynum'])
							except:
								cmt_reply_cnt = '0'
							long_comment = '0'
							# last_modify_date = self.p_time(time.time())
							source_url = film_url
							tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
							       cmt_reply_cnt, long_comment, last_modify_date, source_url]
							print '|'.join(tmp)
							results.append([x.encode('gbk', 'ignore') for x in tmp])
						break
					except Exception as e:
						retry -= 1
						if retry == 0:
							print e
							return results
						else:
							continue
			return results

	def get_cid(self, film_url):
		retry = 10
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'v.qq.com'
				text = requests.get(film_url, proxies=self.GetProxies(), timeout=10).text
				# print text
				if u'itemprop="episode"' in text:
					p = re.compile('itemprop="episode".*?vid=(.*?)"', re.S)
					cid = re.findall(p, text)[0]
					return cid
				else:
					cid = film_url.split('/')[-1].split('.')[0]
					return cid
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue

	def get_film_id(self, film_url):  # 获取电视剧每一集的id
		retry = 10
		while 1:
			try:
				text = requests.get(film_url, proxies=self.GetProxies(), timeout=10).text
				p0 = re.compile('itemprop="episode".*?<a href="http://v\.qq\.com/x/cover/.*?vid=(.*?)"', re.S)
				vids = []
				items = re.findall(p0, text)
				if len(items) == 0:
					p1 = re.compile('https://v\.qq\.com/detail/.*?/(.*?)\.')
					vid = re.findall(p1, film_url)[0]
					vids.append(vid)
					break
				else:
					vids.extend(items)
					break
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
		pool = Pool(10)
		ss = pool.map(self.get_article_id, vids)
		pool.close()
		pool.join()
		videoids = filter(lambda x: x is not None, ss)
		return videoids
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue
	
	def get_comments_total(self, film_url, product_number, plat_number):  # 获取某个视频的所有评论
		videoids = self.get_film_id(film_url)
		if videoids is None:
			return '0'
		print u'共有 %d 集' % len(videoids)
		for vid in videoids:
			result = self.get_comment_short(film_url, vid, product_number, plat_number)
			if result is not None:

				with open('data_comments.csv', 'a') as f:
					writer = csv.writer(f, lineterminator='\n')
					writer.writerows(result)

				# # self.save_sql('t_comments_pub', result)  # 手动修改需要录入的库的名称
				# print u'%s 开始录入数据库' % product_number
				# self.save_sql('T_COMMENTS_PUB', result)  # 手动修改需要录入的库的名称
				# print u'%s 录入数据库完毕' % product_number

	def get_article_id(self, vid):
		url = "https://ncgi.video.qq.com/fcgi-bin/video_comment_id"
		querystring = {"otype": "json", "op": "3", "vid": vid, "cid": vid}
		retry = 10
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'ncgi.video.qq.com'
				text = requests.get(url, headers=headers, proxies=self.GetProxies(), params=querystring,
				                    timeout=10).text
				p0 = re.compile('"comment_id":"(\d+?)",')
				comment_id = re.findall(p0, text)[0]
				return comment_id
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P02'])
	for j in s:
		print j[1],j[0]
		spider.get_comments_total(j[0], j[1], j[2])
	spider.db.db.close()
