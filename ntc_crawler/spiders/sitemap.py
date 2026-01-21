import scrapy
import json
import os
from collections import defaultdict
from urllib.parse import urlparse

class SitemapSpider(scrapy.Spider):
    name = "sitemap"
    allowed_domains = ["ntc.net.np"]
    start_urls = ["https://www.ntc.net.np"]
    
    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "DEPTH_LIMIT": 3,
        "CLOSESPIDER_PAGECOUNT": 200,
        "ROBOTSTXT_OBEY": False,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sitemap_data = {}  # Store all pages
        self.parent_map = defaultdict(list)  # Track parent-child relationships
        
        # Create output directory for individual node files
        self.output_dir = "sitemap_nodes"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def should_follow(self, url):
        """Check if URL should be followed"""
        skip_extensions = ['.pdf', '.mp4', '.mp3', '.zip', '.doc', '.docx', 
                           '.xls', '.xlsx', '.ppt', '.pptx', '.jpg', '.png', 
                           '.gif', '.jpeg', '.avi', '.mov', '.wmv']
        return not any(url.lower().endswith(ext) for ext in skip_extensions)
    
    def parse(self, response):
        # Check if response is HTML
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()
        
        if 'text/html' not in content_type:
            return  # Skip non-HTML content
        
        current_url = response.url
        parent_url = response.meta.get("parent_url", None)
        depth = response.meta.get("depth", 0)
        
        # Extract links from current page
        child_urls = []
        for href in response.css("a::attr(href)").getall():
            if href and not any(href.startswith(s) for s in ['mailto:', 'tel:', 'javascript:', '#']):
                full_url = response.urljoin(href)
                child_urls.append(full_url)
        
        # Create node data
        node = {
            "url": current_url,
            "title": response.css("title::text").get("").strip(),
            "status": response.status,
            "depth": depth,
            "parent": parent_url,
            "child_urls": child_urls  # Store URLs for later tree building
        }
        
        # Store in memory
        self.sitemap_data[current_url] = node
        
        # Track parent-child relationship
        if parent_url and parent_url in self.sitemap_data:
            self.parent_map[parent_url].append(current_url)
        
        # Save individual node to file
        node_id = len(self.sitemap_data)
        safe_filename = f"node_{node_id:04d}.json"
        with open(f"{self.output_dir}/{safe_filename}", 'w', encoding='utf-8') as f:
            json.dump(node, f, indent=2, ensure_ascii=False)
        
        # Follow links
        for link in child_urls:
            if self.should_follow(link):
                yield response.follow(
                    link, 
                    callback=self.parse, 
                    meta={"parent_url": current_url}
                )
    
    def closed(self, reason):
        """Called when spider finishes"""
        self.logger.info(f"Spider closed. Total pages crawled: {len(self.sitemap_data)}")
        
        # Build hierarchical tree
        tree = self.build_tree(self.start_urls[0])
        
        # Save tree structure
        with open('sitemap_tree.json', 'w', encoding='utf-8') as f:
            json.dump(tree, f, indent=2, ensure_ascii=False)
        
        # Save flat list (your current format)
        with open('sitemap_flat.json', 'w', encoding='utf-8') as f:
            flat_list = list(self.sitemap_data.values())
            json.dump(flat_list, f, indent=2, ensure_ascii=False)
        
        # Create text visualization
        with open('sitemap_tree.txt', 'w', encoding='utf-8') as f:
            self.write_tree_text(tree, f)
        
        # Print summary
        self.logger.info(f" Output files created:")
        self.logger.info(f"    {self.output_dir}/ - Individual node files")
        self.logger.info(f"    sitemap_tree.json - Hierarchical tree")
        self.logger.info(f"    sitemap_flat.json - Flat list")
        self.logger.info(f"    sitemap_tree.txt - Text visualization")
    
    def build_tree(self, url):
        """Recursively build tree structure"""
        if url not in self.sitemap_data:
            return None
        
        node = self.sitemap_data[url].copy()
        
        # Get actual children that were crawled
        children = []
        for child_url in self.parent_map.get(url, []):
            child_node = self.build_tree(child_url)
            if child_node:
                children.append(child_node)
        
        # Replace child_urls with actual nested children
        node['children'] = children
        del node['child_urls']  # Remove the flat URL list
        
        return node
    
    def write_tree_text(self, node, file, prefix="", is_last=True):
        """Write tree in text format with ASCII art"""
        if not node:
            return
        
        connector = "└── " if is_last else "├── "
        title = node.get('title', 'No title')[:50]  # Truncate long titles
        file.write(f"{prefix}{connector}{title}\n")
        file.write(f"{prefix}    URL: {node['url']}\n")
        
        children = node.get('children', [])
        for i, child in enumerate(children):
            extension = "    " if is_last else "│   "
            self.write_tree_text(
                child, 
                file, 
                prefix + extension, 
                i == len(children) - 1
            )