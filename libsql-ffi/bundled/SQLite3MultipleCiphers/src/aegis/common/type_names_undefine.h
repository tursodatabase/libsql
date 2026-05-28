/*
** Name:        type_names_undefine.h
** Purpose:     Undefines for AEGIS type names
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

/*
** NOTE:
** Do NOT use include guards, because including this header
** multiple times is intended behaviour.
*/

/* Undefine AES block length */
#undef AES_BLOCK_LENGTH

/* Undefine type names */
#undef AEGIS_AES_BLOCK_T
#undef AEGIS_BLOCKS
#undef AEGIS_STATE
#undef AEGIS_MAC_STATE
