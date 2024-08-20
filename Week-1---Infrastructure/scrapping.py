import os
import pandas as pd


class ConfigObject:
    def __init__(self, save_path: str, website_path: str, page_number: int, dataframe: pd.DataFrame) -> None:
        self.save_path = save_path
        self.website_path = website_path
        self.page_number = page_number
        self.dataframe = dataframe
        
    def getSavePath(self) -> str:
        return self.save_path
        
    def getWebsitePath(self) -> str:
        return self.website_path
        
    def getPageNumber(self) -> int:
        return self.page_number
    
    def getDataFrame(self) -> pd.DataFrame:
        return self.dataframe
    
    def setSavePath(self, save_path) -> None:
        self.save_path = save_path
        
    def __str__(self) -> str:
        return f"""save_path: {self.save_path}\nwebsite_path: {self.website_path}\n
                   page_number: {self.page_number}\ndataframe: {self.dataframe.shape()}"""
                   
                   

config = ConfigObject("/usr/config/", "http://tiki.vn", 23, "")
config.setSavePath("/tmp/config/.aws")
config.getSavePath()