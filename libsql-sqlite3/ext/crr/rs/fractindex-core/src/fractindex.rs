extern crate alloc;

use alloc::{
    format,
    string::{String, ToString},
};

pub static BASE_95_DIGITS: &'static str =
    " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

static SMALLEST_INTEGER: &'static str = "A                          ";
static INTEGER_ZERO: &'static str = "a ";

static a_charcode: u8 = 97;
static z_charcode: u8 = 122;
static A_charcode: u8 = 65;
static Z_charcode: u8 = 90;
static min_charcode: u8 = 32;

pub fn key_between(a: Option<&str>, b: Option<&str>) -> Result<Option<String>, &'static str> {
    // configurable digits not yet supported
    let digits = BASE_95_DIGITS;

    a.map(|a| validate_order_key(a)).transpose()?;
    b.map(|b| validate_order_key(b)).transpose()?;
    match (&a, &b) {
        (None, None) => return Ok(Some(String::from(INTEGER_ZERO))),
        (Some(a), Some(b)) => {
            if a > b {
                return Err("key_between - a must be before b");
            }

            let ia = get_integer_part(a)?;
            let ib = get_integer_part(b)?;
            let fa = &a[ia.len()..];
            let fb = &b[ib.len()..];
            if ia == ib {
                return Ok(Some(format!("{}{}", ia, midpoint(fa, Some(fb), digits)?)));
            }

            if let Some(i) = increment_integer(ia, digits)? {
                if i < b.to_string() {
                    return Ok(Some(i));
                }

                return Ok(Some(format!("{}{}", ia, midpoint(fa, None, digits)?)));
            } else {
                return Err("Cannot increment anymore");
            }
        }
        (None, Some(b)) => {
            let ib = get_integer_part(b)?;
            let fb = &b[ib.len()..];
            if ib == SMALLEST_INTEGER {
                return Ok(Some(format!("{}{}", ib, midpoint("", Some(fb), digits)?)));
            }
            if ib < b {
                return Ok(Some(String::from(ib)));
            }
            if let Some(res) = decrement_integer(ib, digits)? {
                return Ok(Some(res));
            } else {
                return Err("cannot decrement anymore");
            }
        }
        (Some(a), None) => {
            let ia = get_integer_part(a)?;
            let fa = &a[ia.len()..];
            let i = increment_integer(ia, digits)?;
            if i.is_none() {
                return Ok(Some(format!("{}{}", ia, midpoint(fa, None, digits)?)));
            } else {
                return Ok(i);
            }
        }
    }
}

fn midpoint(a: &str, b: Option<&str>, digits: &str) -> Result<String, &'static str> {
    b.map(|b| {
        if a > b {
            Err("midpoint - a must be before b")
        } else {
            Ok(())
        }
    })
    .transpose()?;

    let a_bytes = a.as_bytes();
    let b_last_char = b.map(|b| {
        let b_bytes = b.as_bytes();
        b_bytes[b_bytes.len() - 1]
    });
    if a_bytes.len() > 0 && a_bytes[a_bytes.len() - 1] == min_charcode
        || (b_last_char.map_or(false, |b| b == min_charcode))
    {
        return Err("midpoint - a or b must not end with ' ' (space)");
    }

    if let Some(b) = b {
        let mut n = 0;
        let b_bytes = b.as_bytes();

        while (if n < a_bytes.len() {
            a_bytes[n]
        } else {
            min_charcode
        } == b_bytes[n])
        {
            n += 1;
        }

        if n > 0 {
            return Ok(format!(
                "{}{}",
                &b[0..n],
                midpoint(
                    if n > a.len() { "" } else { &a[n..] },
                    if n > b.len() { None } else { Some(&b[n..]) },
                    digits
                )?
            ));
        }
    }

    let digit_a = if a.len() > 0 {
        digits.find(a_bytes[0] as char)
    } else {
        Some(0)
    };

    if let Some(digit_a) = digit_a {
        let digit_b = match b {
            Some(b) => digits.find(b.as_bytes()[0] as char),
            None => Some(digits.len()),
        };
        if let Some(digit_b) = digit_b {
            if digit_b - digit_a > 1 {
                let mid_digit = round(0.5 * (digit_a + digit_b) as f64);
                return Ok(String::from(&digits[mid_digit..mid_digit + 1]));
            } else {
                if b.map_or(false, |b| b.len() > 1) {
                    return Ok(String::from(&b.unwrap()[0..1]));
                } else {
                    return Ok(format!(
                        "{}{}",
                        &digits[digit_a..digit_a + 1],
                        midpoint(if a == "" { a } else { &a[1..] }, None, digits)?
                    ));
                }
            }
        } else {
            return Err("midpoint - b has invalid digits");
        }
    } else {
        return Err("midpoint - a has invalid digits");
    }
}

fn round(d: f64) -> usize {
    let tenx = (d * 10.0) as usize;
    let truncated = d as usize;
    if tenx - (truncated * 10) >= 5 {
        truncated + 1
    } else {
        truncated
    }
}

fn validate_order_key(key: &str) -> Result<(), &'static str> {
    if key == SMALLEST_INTEGER {
        return Err("Key is too small");
    }
    let i = get_integer_part(key)?;
    let f = &key[i.len()..];
    let as_bytes = f.as_bytes();
    if as_bytes.len() > 0 && as_bytes[as_bytes.len() - 1] == min_charcode {
        return Err("Fractional part should not end with ' ' (space)");
    }

    Ok(())
}

fn get_integer_part(key: &str) -> Result<&str, &'static str> {
    // as_bytes is safe as we control the alphabet
    let integer_part_len = get_integer_len(key.as_bytes()[0])?;
    if integer_part_len > key.len() as u8 {
        return Err("integer part of key is too long");
    }
    return Ok(&key[0..integer_part_len as usize]);
}

fn get_integer_len(head: u8) -> Result<u8, &'static str> {
    if head >= a_charcode && head <= z_charcode {
        return Ok(head - a_charcode + 2);
        // >= A and <= Z
    } else if head >= A_charcode && head <= Z_charcode {
        return Ok(Z_charcode - head + 2);
    } else {
        return Err("head is out of range");
    }
}

fn validate_integer(i: &str) -> Result<(), &'static str> {
    if i.len() as u8 != get_integer_len(i.as_bytes()[0])? {
        return Err("invalid integer part of order key");
    }

    return Ok(());
}

fn increment_integer(x: &str, digits: &str) -> Result<Option<String>, &'static str> {
    validate_integer(x)?;

    let head = &x[0..1];
    let mut digs = String::from(&x[1..]);
    let mut carry = true;

    let mut i = digs.len() as i32 - 1;
    while carry && i >= 0 {
        let ui = i as usize;
        let temp = digits.find(&digs[ui..ui + 1]);
        if let Some(temp) = temp {
            let d = temp + 1;

            if d == digits.len() {
                digs.replace_range(ui..ui + 1, &digits[0..1]);
            } else {
                digs.replace_range(ui..ui + 1, &digits[d..d + 1]);
                carry = false;
            }
        } else {
            return Err("invalid digit");
        }
        i -= 1;
    }

    if carry {
        if head == "Z" {
            return Ok(Some(String::from(INTEGER_ZERO)));
        }
        if head == "z" {
            return Ok(None);
        }
        let h = head.as_bytes()[0] + 1;
        if h > a_charcode {
            digs.push(digits.chars().nth(0).unwrap());
        } else {
            digs.pop();
        }
        return Ok(Some(format!("{}{}", h as char, digs)));
    } else {
        return Ok(Some(format!("{}{}", head, digs)));
    }
}

fn decrement_integer(x: &str, digits: &str) -> Result<Option<String>, &'static str> {
    validate_integer(x)?;

    let head = &x[0..1];
    let mut digs = String::from(&x[1..]);
    let mut borrow = true;

    let mut i = digs.len() as i32 - 1;
    while borrow && i >= 0 {
        let ui = i as usize;
        if let Some(temp) = digits.find(&digs[ui..ui + 1]) {
            let d = temp as i32 - 1;
            if d == -1 {
                digs.replace_range(ui..ui + 1, &digits[digits.len() - 1..digits.len()]);
            } else {
                let ud = d as usize;
                digs.replace_range(ui..ui + 1, &digits[ud..ud + 1]);
                borrow = false;
            }
        } else {
            return Err("invalid digit");
        }
        i -= 1;
    }

    if borrow {
        if head == "a" {
            return Ok(Some(format!(
                "Z{}",
                &digits[digits.len() - 1..digits.len()]
            )));
        }
        if head == "A" {
            return Ok(None);
        }
        let h = head.as_bytes()[0] - 1;
        if h < Z_charcode {
            digs.push_str(&digits[digits.len() - 1..digits.len()]);
        } else {
            digs.pop();
        }
        return Ok(Some(format!("{}{}", h as char, digs)));
    } else {
        return Ok(Some(format!("{}{}", head, digs)));
    }
}

#[cfg(test)]
mod tests {
    extern crate alloc;
    use crate::fractindex;
    use alloc::vec;
    use alloc::{string::String, vec::Vec};
    use rand::distributions::{Distribution, Uniform};

    use super::SMALLEST_INTEGER;
    #[test]
    fn validate_order_key() {
        // too small
        assert_eq!(
            fractindex::validate_order_key(SMALLEST_INTEGER),
            Err("Key is too small")
        );

        // do generation and validation feedback loop tests later
        assert_eq!(fractindex::validate_order_key("a0"), Ok(()));
        assert_eq!(fractindex::validate_order_key("a1"), Ok(()));
        assert_eq!(fractindex::validate_order_key("a2"), Ok(()));
        assert_eq!(fractindex::validate_order_key("Zz"), Ok(()));
        assert_eq!(fractindex::validate_order_key("a1V"), Ok(()));
    }

    #[test]
    fn key_between() {
        fn test(a: Option<&str>, b: Option<&str>, exp: Result<Option<String>, &'static str>) {
            let btwn = fractindex::key_between(a, b);
            assert_eq!(btwn, exp);
        }

        test(None, None, Ok(Some(String::from("a "))));
        test(None, Some("a "), Ok(Some(String::from("Z~"))));
        test(None, Some("Z~"), Ok(Some(String::from("Z}"))));
        test(Some("a "), None, Ok(Some(String::from("a!"))));
        test(Some("a!"), None, Ok(Some(String::from("a\""))));
        test(Some("a0"), Some("a1"), Ok(Some(String::from("a0P"))));
        test(Some("a1"), Some("a2"), Ok(Some(String::from("a1P"))));
        test(Some("a0V"), Some("a1"), Ok(Some(String::from("a0k"))));
        test(Some("Z~"), Some("a "), Ok(Some(String::from("Z~P"))));
        test(Some("Z~"), Some("a!"), Ok(Some(String::from("a "))));
        test(None, Some("Y  "), Ok(Some(String::from("X~~~"))));
        test(Some("b~~"), None, Ok(Some(String::from("c   "))));
        test(Some("a0"), Some("a0V"), Ok(Some(String::from("a0;"))));
        test(Some("a0"), Some("a0G"), Ok(Some(String::from("a04"))));
        test(Some("b125"), Some("b129"), Ok(Some(String::from("b127"))));
        test(Some("a0"), Some("a1V"), Ok(Some(String::from("a1"))));
        test(Some("Z~"), Some("a 1"), Ok(Some(String::from("a "))));
        test(None, Some("a0V"), Ok(Some(String::from("a0"))));
        test(None, Some("b999"), Ok(Some(String::from("b99"))));
        test(
            None,
            Some("A                          "),
            Err("Key is too small"),
        );
        test(
            None,
            Some("A                          !"),
            Ok(Some(String::from("A                           P"))),
        );
        test(
            Some("zzzzzzzzzzzzzzzzzzzzzzzzzzy"),
            None,
            Ok(Some(String::from("zzzzzzzzzzzzzzzzzzzzzzzzzzz"))),
        );
        test(
            Some("z~~~~~~~~~~~~~~~~~~~~~~~~~~"),
            None,
            Ok(Some(String::from("z~~~~~~~~~~~~~~~~~~~~~~~~~~P"))),
        );
        test(
            Some("a0 "),
            None,
            Err("Fractional part should not end with ' ' (space)"),
        );
        test(
            Some("a0 "),
            Some("a1"),
            Err("Fractional part should not end with ' ' (space)"),
        );
        test(Some("0"), Some("1"), Err("head is out of range"));
        test(
            Some("a1"),
            Some("a0"),
            Err("key_between - a must be before b"),
        );
    }

    #[test]
    fn generate_insert_order() {
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..5);
        // 1. generate a list of indices
        // 2. Permute the copy by moving items around
        // 3. Get new indice of the item moved for each move
        // 4. order by indice and compare to original list

        let mut prev: Option<String> = None;
        let mut indices: Vec<String> = vec![];
        for _ in 0..5 {
            prev = fractindex::key_between(prev.as_deref().map(|x| &x[..]), None).unwrap();
            indices.push(String::from(prev.as_deref().unwrap()));
        }

        let mut sorted = indices.clone();
        sorted.sort();
        assert_eq!(vec_compare(&sorted, &indices), true);

        let mut i = 0;
        // Run through 1k random re-orderings and ensure the list is always sorted
        // correctly by fractional index
        while i < 10 {
            let from_index = die.sample(&mut rng);
            let to_index = die.sample(&mut rng);
            if from_index == to_index {
                continue;
            }

            let fract_index = fractindex::key_between(
                if to_index == 0 {
                    None
                } else {
                    indices.get(to_index - 1).map(|x| &x[..])
                },
                indices.get(to_index).map(|x| &x[..]),
            )
            .unwrap()
            .unwrap();

            indices.insert(to_index, fract_index);
            indices.remove(from_index);
            let mut sorted = indices.clone();
            sorted.sort();
            assert_eq!(vec_compare(&sorted, &indices), true);

            i += 1;
        }
    }

    fn vec_compare(va: &[String], vb: &[String]) -> bool {
        (va.len() == vb.len()) && va.iter().zip(vb).all(|(a, b)| a == b)
    }
}
