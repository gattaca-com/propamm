// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.24;

interface IKipseli {
    function swap(
        address tokenIn,
        uint256 amountIn,
        address tokenOut,
        address recipient
    ) external returns (uint256);
}

/// Wrapper around the Kipseli pAMM (0x5CDbE594…) that validates minOut.
/// Pulls tokenIn directly from msg.sender into the pool, then calls swap
/// with msg.sender as recipient. Reverts if tokenOut received is below minOut.
contract KipseliGuard {
    address public constant KIPSELI = 0x5CDbE59400Cc2EFDCC2B54acca4a99FE00dD588c;

    error AmountOutTooLow(uint256 received, uint256 minOut);
    error TransferFailed();

    function swap(
        address tokenIn,
        uint256 amountIn,
        address tokenOut,
        uint256 minOut
    ) external returns (uint256 received) {
        _safeTransferFrom(tokenIn, msg.sender, KIPSELI, amountIn);

        received = IKipseli(KIPSELI).swap(tokenIn, amountIn, tokenOut, msg.sender);

        if (received < minOut) revert AmountOutTooLow(received, minOut);
    }

    function _safeTransferFrom(address token, address from, address to, uint256 value) private {
        (bool ok, bytes memory data) = token.call(
            abi.encodeWithSelector(0x23b872dd, from, to, value)
        );
        if (!ok || (data.length != 0 && !abi.decode(data, (bool)))) revert TransferFailed();
    }
}
