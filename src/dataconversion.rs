use std::borrow::Cow;

const SWIPL_CONTROL_CHAR_A: char = 7 as char;
const SWIPL_CONTROL_CHAR_B: char = 8 as char;
const SWIPL_CONTROL_CHAR_F: char = 12 as char;
const SWIPL_CONTROL_CHAR_V: char = 11 as char;

pub fn prolog_string_to_string(s: &str) -> Cow<str> {
    let mut result: Option<String> = None;
    let mut escaping = false;
    let mut characters = s.char_indices();
    while let Some((ix, c)) = characters.next() {
        if escaping {
            let result = result.as_mut().unwrap();
            match c {
                '\\' => result.push('\\'),
                '\"' => result.push('\"'),
                'x' => result.push(unescape_legacy_prolog_escape_sequence(&mut characters)),
                'a' => result.push(SWIPL_CONTROL_CHAR_A),
                'b' => result.push(SWIPL_CONTROL_CHAR_B),
                't' => result.push('\t'),
                'n' => result.push('\n'),
                'v' => result.push(SWIPL_CONTROL_CHAR_V),
                'f' => result.push(SWIPL_CONTROL_CHAR_F),
                'r' => result.push('\r'),
                _ => panic!("unknown prolog escape code in string"),
            }

            escaping = false;
        } else {
            if c == '\\' {
                escaping = true;
                if result.is_none() {
                    let mut r = String::with_capacity(s.len());
                    r.push_str(&s[..ix]);
                    result = Some(r);
                }
            } else if let Some(result) = result.as_mut() {
                result.push(c);
            }
        }
    }

    match result {
        Some(result) => {
            Cow::Owned(result)
        }
        None => Cow::Borrowed(s),
    }
}

fn unescape_legacy_prolog_escape_sequence(
    characters: &mut impl Iterator<Item = (usize, char)>,
) -> char {
    let mut digits: String = String::new();
    loop {
        let (_, digit) = characters.next().unwrap();
        if digit == '\\' {
            let hex = u32::from_str_radix(&digits, 16).unwrap();
            return char::from_u32(hex).unwrap();
        } else {
            digits.push(digit);
        }
    }
}
