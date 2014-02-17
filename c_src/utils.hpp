#ifndef __MESOS_C_UTILS_HPP__
#define __MESOS_C_UTILS_HPP__

//#include <string>
//#include <vector>

#include "mesos/mesos.pb.h"
#include "erl_nif.h"

template <class T> 
ERL_NIF_TERM pb_obj_to_binary(ErlNifEnv *env, const T& obj)  {
    ErlNifBinary res;
    enif_alloc_binary(obj.ByteSize(), &res); // Review : do I need to dealloc this?
    obj.SerializeToArray(res.data, res.size);
    return enif_make_binary(env, &res);
}

template<typename T> inline bool deserialize(T& ret, void* data, size_t size)
  {
    if (!ret.ParseFromArray(data, size)) {
      printf("Deserialization failed\n");
      return false;
    }
    return true;
}

template<typename T> inline bool deserialize(T& ret, ErlNifBinary* obj) {
  if (obj == NULL) {
    return false;
  }
  return deserialize<T>(ret, obj->data, obj->size);
}

template<typename T> inline bool deserialize2(
      std::vector<T>& ret,
      ErlNifBinary* ent)
  {
    for(int i = 0; i < sizeof(ent) / sizeof(ent[0] ); i++)
    {
      T obj;
      if(!deserialize<T>(obj, &ent[i])){return false;}
      ret.push_back(obj);
    }
    return true;
  }













#endif
