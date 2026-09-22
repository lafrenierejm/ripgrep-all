{
  name = "John";
  age = 30;
  ratio = 1.5;
  enabled = true;
  nothing = null;
  tags = ["developer" "rust" "python"];

  ports = [
    80
    443
  ];

  empty_list = [];
  empty_set = {};
  point = { x = 1; y = "two"; };

  person = {
    name = "Alice";
    age = 25;
  };

  computed.${dynamicKey} = 1;

  description = ''
    line one
    line two
  '';

  base.value = 1;
  derived.a = base.value;
  derived.b = extra.thing;
}
