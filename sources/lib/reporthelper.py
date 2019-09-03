############################################
# ReportStructure
############################################
class ReportStructure:
    
#===========================================    
    def __init__(self,es,report=""):
        self.entities=[]
        self.entitiesHT={}
        self.report=report
        
        res=es.search(index="biac_entity",body={"size":100})        
        for rec in res["hits"]["hits"]:
            self.entities.append(rec["_source"])
            self.entitiesHT[rec["_source"]["key"]]=rec["_source"]
            
#===========================================
    def getEntities(self):
        return self.entities

#===========================================    
#    def getEntity(self,lot,contract='',technic=''):
#for rec in self.entities:
#            if rec["lot"]==lot and rec["contract"]==contract and rec["technic"]==technic:
#            return rec
#        return None

#===========================================    
    def getEntity(self,key):
        return self.entitiesHT.get(key,None)    
    
#===========================================    
    def getHeader(self):
        return self.entitiesHT[self.report]["header"]
    
#===========================================    
    def getKPI304Config(self):
        ent=self.getEntity(self.report)
        print(ent)
        if ent !=None and "kpi304" in ent["kpis"]:
            return (ent["kpis"]["kpi304"]["total"],ent["kpis"]["kpi304"]["objective"])
        return (None,None)

#===========================================    
    def getMaximoConfig(self):
        ent=self.getEntity(self.report)
        if ent !=None and "maximo" in ent["kpis"]:
            return (str(ent["lot"]),ent["contract"],ent["technic"],ent["kpis"]["maximo"][0]["screen_name"])
        return (str(ent["lot"]),"","","")
    
#===========================================    
    def getSpotCheckConfig(self):
        ent=self.getEntity(self.report)
        if ent !=None and "spot" in ent["kpis"]:
            return (str(ent["lot"]),ent["contract"],ent["technic"],ent["kpis"]["spot"])
        else:
            return (str(ent["lot"]),ent["contract"],ent["technic"],[])
        return None    

#===========================================    
    def getHoneyConfig(self):
        ent=self.getEntity(self.report)
        if ent !=None and "honey" in ent["kpis"]:
            return (str(ent["lot"]),ent["contract"],ent["technic"],ent["kpis"]["honey"])
        else:
            return (str(ent["lot"]),ent["contract"],ent["technic"],[])
        return None
    
#===========================================    
    def getKPI500Config(self,name,bacservice):        
        ent=self.getEntity(self.report)

        if name =="Dirk Van Leemput":
            name="Stefaan Pletinckx"
        if name =="Wim Sintubin":
            name="Stefaan Pletinckx"
        
        name=name.replace("é","e")
        #print("KPI 500 NAME:"+name+" BAC:"+bacservice)

        for rec in self.entities:
            if "header" not in rec:
                continue
            author=rec["header"]["auteur"]
            service=rec["header"]["service"]
#            print(">>>>>>>>>> <%s> <%s>" %(author,service))
            if author==name and service==bacservice:
                return rec

            if "lot" in rec and (rec["lot"]==1 or rec["lot"]==3) and  author==name:
                #print("LOT 1 OR 3"*100)
                return rec


            
        for rec in self.entities:
            if "header" not in rec:
                continue

            if "kpi500exception" in rec["header"]:
                #print(rec["header"]["kpi500exception"])
                for pair in rec["header"]["kpi500exception"]:
                    if name==pair["name"] and pair["service"]==bacservice:
                        return rec 
        return None   