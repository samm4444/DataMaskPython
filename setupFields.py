import json

masks = [{"id": "redact",
          "displayName":"Redact",
          "description": "Replace every character in the input with another for e.g: #",
          "params": [
            {
            "id": "replacement",
            "displayName": "Replacement",
            "description": "The replacement character",
            "type": "str"
          }]},
         {"id": "partial",
          "displayName":"Partial",
          "description": "Replace charcters with another, optionally leaving some characters visable at the start and/or end of the input.",
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
          "description": "Replace the parts that match a REGEX with something else.",
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
          }]},
         {"id": "scrambleInt",
          "displayName":"Scramble Integer",
          "description": "Replace numbers in a sequence with random numbers. E.g. 073892564321 -> 88644284274",
          "params": [
            {
            "id": "min",
            "displayName": "Minimum",
            "description": "The smallest a number in the sequence can be.",
            "type": "int"
          },
          {
            "id": "max",
            "displayName": "Maximum",
            "description": "The largest a number in the sequence can be.",
            "type": "int"
          },
          {
            "id": "length",
            "displayName": "Length",
            "description": "The Length of the masked sequence. Type None to keep the length of the input.",
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





