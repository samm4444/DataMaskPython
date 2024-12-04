import json

masks = [{"id": "redact",
          "displayName":"Redact",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          }]},
         {"id": "partial",
          "displayName":"Partial",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          },
          {
            "id": "visiblePrefix",
            "displayName": "Visible Prefix",
            "description": "The Number of visible characters at the start of the text",
            "type": "int"
          },
          {
            "id": "visibleSuffix",
            "displayName": "Visible Suffix",
            "description": "The Number of visible characters at the end of the text",
            "type": "int"
          }]},
         {"id": "regex",
          "displayName":"Regular Expression",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          },
          {
            "id": "pattern",
            "displayName": "Regular Expression",
            "description": "The regualar expression pattern to match",
            "type": "str"
          }]}]

def getMaskConfig() -> dict:
  config = {}
  print("Select Mask Type")
  for i,mask in enumerate(masks):
    print(str(i+1) + ". " + mask["displayName"])
  
  while True:
    try:
      maskID = int(input("=>"))
      mask = masks[maskID-1]
      break
    except TypeError and KeyError:
      continue
  config["maskingType"] = mask["id"]
  for param in mask["params"]:
    paramId = param["id"]
    paramDisplayName = param["displayName"]
    paramDescription = param["description"]
    paramType = param["type"]

    print(paramDisplayName)
    print(paramDescription)
    if paramType == "str":
      paramValue = input("=>")
      config[paramId] = paramValue
    elif paramType == "int":
      try:
        paramValue = int(input("=>"))
        config[paramId] = paramValue
      except TypeError:
        print("Enter a number")


  return config





