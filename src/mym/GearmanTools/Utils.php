<?php

/**
 * @copyright 2013, Mikhail Yurasov <me@yurasov.me>
 */

namespace mym\GearmanTools;

class Utils
{
  /**
   * Pack message (serialize and compress)
   *
   * @param mixed $data
   * @return string
   */
  public static function packMessage($data)
  {
    return gzcompress(serialize($data));
  }

  /**
   * Unpack message packed with packMessage()
   *
   * @param string $message
   * @return mixed
   */
  public static function unpackMessage($message)
  {
    return unserialize(gzuncompress($message));
  }
}