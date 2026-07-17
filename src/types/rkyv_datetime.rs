use chrono::{Datelike, NaiveDate};

pub struct ChronoDateOption;

impl rkyv::with::ArchiveWith<Option<NaiveDate>> for ChronoDateOption {
    type Archived = rkyv::rend::i32_le;
    type Resolver = ();

    fn resolve_with(
        field: &Option<NaiveDate>,
        _resolver: Self::Resolver,
        out: rkyv::Place<Self::Archived>,
    ) {
        let val = field.as_ref().map_or(0, Datelike::num_days_from_ce);
        out.write(rkyv::rend::i32_le::from_native(val));
    }
}

impl<S: rkyv::rancor::Fallible + ?Sized> rkyv::with::SerializeWith<Option<NaiveDate>, S>
    for ChronoDateOption
{
    fn serialize_with(
        _field: &Option<NaiveDate>,
        _serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        Ok(())
    }
}

impl<D: rkyv::rancor::Fallible + ?Sized>
    rkyv::with::DeserializeWith<rkyv::rend::i32_le, Option<NaiveDate>, D> for ChronoDateOption
{
    fn deserialize_with(
        field: &rkyv::rend::i32_le,
        _deserializer: &mut D,
    ) -> Result<Option<NaiveDate>, D::Error> {
        let val = field.to_native();
        if val == 0 {
            Ok(None)
        } else {
            Ok(NaiveDate::from_num_days_from_ce_opt(val))
        }
    }
}
