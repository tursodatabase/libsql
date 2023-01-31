/// Value of a single database cell
#[derive(Clone, Debug)]
pub enum CellValue {
    Text(String),
    Float(f64),
    Number(i64),
    Bool(bool),
    Null,
}

impl std::fmt::Display for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CellValue::Text(s) => write!(f, "\"{s}\""),
            CellValue::Float(d) => write!(f, "{d}"),
            CellValue::Number(n) => write!(f, "{n}"),
            CellValue::Bool(b) => write!(f, "{}", if *b { "1" } else { "0" }),
            CellValue::Null => write!(f, "null"),
        }
    }
}

impl From<()> for CellValue {
    fn from(_: ()) -> CellValue {
        CellValue::Null
    }
}

macro_rules! impl_from_cell_value {
    ($typename: ty, $variant: ident) => {
        impl From<$typename> for CellValue {
            fn from(t: $typename) -> CellValue {
                CellValue::$variant(t.into())
            }
        }
    };
}

impl_from_cell_value!(String, Text);
impl_from_cell_value!(&str, Text);

impl_from_cell_value!(i8, Number);
impl_from_cell_value!(i16, Number);
impl_from_cell_value!(i32, Number);
impl_from_cell_value!(i64, Number);

impl_from_cell_value!(u8, Number);
impl_from_cell_value!(u16, Number);
impl_from_cell_value!(u32, Number);

impl_from_cell_value!(f32, Float);
impl_from_cell_value!(f64, Float);

impl_from_cell_value!(bool, Bool);
