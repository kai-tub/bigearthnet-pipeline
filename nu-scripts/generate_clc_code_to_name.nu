
export def "load clc-metadata" [] nothing -> table {
  open clc_metadata.json
}

export def generate_fill_clc_code18_to_clc_name [--table-name: string = "clc_code_18_to_clc_name"] nothing -> string {
  # Get file location!
  let data = load clc-metadata | get clc_data | each {|r| $"\t\('($r.code)'::clc_code_18, '($r.name_clc)'\)"} | str join ",\n"
  print $"
  insert into ($table_name) values
  ($data);
  "
}

def "main generate_fill_clc_code18_to_clc_name" [] {
  generate_fill_clc_code18_to_clc_name 
}

export def generate_fill_clc_code18_to_ben19_name [--table-name: string = "clc_code_18_to_ben19_name"] nothing -> string {
  let data  = load clc-metadata | get clc_data | each {|r| $"\t\('($r.code)'::clc_code_18, '($r.name_19)'\)"} | str join ",\n"
  print $"
  insert into ($table_name) values
  ($data);
  "
}

def "main generate_fill_clc_code18_to_ben19_name" [] {
  generate_fill_clc_code18_to_ben19_name
}

def main [] {}
