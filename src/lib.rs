use std::io;

use std::path::Path;
use std::path::PathBuf;

use io::ErrorKind;

use io::BufRead;

use redb::Database;
use redb::Durability;
use redb::TableDefinition;
use redb::WriteTransaction;

use arrow_ipc::reader::StreamReader;

use arrow_array::ArrayRef;
use arrow_array::GenericStringArray;

use arrow_array::cast::AsArray;

use arrow_array::RecordBatch;

pub fn batch2tx(
    b: &RecordBatch,
    mut tx: WriteTransaction,
    tdef: TableDefinition<&[u8], &str>,
    offset: u32,
) -> Result<u32, io::Error> {
    let nc: usize = b.num_columns();
    if nc < 1 {
        return Err(io::Error::new(ErrorKind::InvalidInput, "too few columns"));
    }

    let avals: &ArrayRef = b.column(0);
    let ovals: Option<&GenericStringArray<i32>> = avals.as_string_opt();
    let Some(vals) = ovals else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "unexpected val type",
        ));
    };

    tx.set_durability(Durability::None)
        .map_err(io::Error::other)?;

    let (count, _) = vals.iter().enumerate().try_fold(
        (0u32, tx.open_table(tdef).map_err(io::Error::other)?),
        |(cnt, mut tab), (ix, ostr)| {
            let Some(item) = ostr else {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "the string missing",
                ));
            };

            let i: u32 = ix as u32;
            let key: u32 = i.saturating_add(offset);
            let encoded: [u8; 4] = key.to_be_bytes();

            let s: &[u8] = &encoded;

            tab.insert(s, item).map_err(io::Error::other)?;

            Ok((cnt.saturating_add(1), tab))
        },
    )?;

    tx.commit().map_err(io::Error::other)?;

    Ok(count)
}

pub fn ibat2db<I>(
    ibat: I,
    db: &Database,
    tdef: TableDefinition<&[u8], &str>,
) -> Result<u32, io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    let mut offset: u32 = 0;

    for rslt in ibat {
        let rbat: RecordBatch = rslt?;
        let wtx: WriteTransaction = db.begin_write().map_err(io::Error::other)?;

        let wrote_cnt: u32 = batch2tx(&rbat, wtx, tdef, offset)?;

        offset = offset.saturating_add(wrote_cnt);
    }

    Ok(offset)
}

pub fn brdr2ibat<R>(
    brdr: R,
    projection: Option<Vec<usize>>,
) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error>
where
    R: BufRead,
{
    StreamReader::try_new(brdr, projection)
        .map_err(io::Error::other)
        .map(|sr| sr.map(|item| item.map_err(io::Error::other)))
}

pub fn brdr2ibat_default<R>(
    brdr: R,
) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error>
where
    R: BufRead,
{
    brdr2ibat(brdr, None)
}

pub fn stdin2ibat_default()
-> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error> {
    brdr2ibat_default(io::stdin().lock())
}

pub const CACHE_SIZE_DEFAULT: usize = 16777216;

#[derive(Debug, Clone, Copy)]
pub struct CacheSize(pub usize);

impl Default for CacheSize {
    fn default() -> Self {
        Self(CACHE_SIZE_DEFAULT)
    }
}

impl CacheSize {
    pub fn path2db_default<P>(self, p: P) -> Result<Database, io::Error>
    where
        P: AsRef<Path>,
    {
        Database::builder()
            .set_cache_size(self.0)
            .create(p)
            .map_err(io::Error::other)
    }
}

pub fn path2db_default<P>(p: P) -> Result<Database, io::Error>
where
    P: AsRef<Path>,
{
    CacheSize::default().path2db_default(p)
}

pub struct SimpleConfig {
    pub table_name: String,
    pub redb_out_path: PathBuf,
}

impl SimpleConfig {
    pub fn stdin2ipc2redb(self) -> Result<(), io::Error> {
        let ibat = stdin2ibat_default()?; // iter<RecordBatch?>
        let db: Database = path2db_default(self.redb_out_path)?;
        let tdef: TableDefinition<&[u8], &str> = TableDefinition::new(&self.table_name);
        ibat2db(ibat, &db, tdef)?;
        Ok(())
    }
}
